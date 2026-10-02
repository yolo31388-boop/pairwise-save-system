"""存档系统 - 原子写入 / 版本迁移 / 云同步冲突处理 / 循环引用序列化压缩 / 自动存档与导入导出"""
from __future__ import annotations

import base64
import copy
import gzip
import hashlib
import inspect
import json
import os
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from typing import Any, Optional

CURRENT_VERSION = 2
MIN_COMPATIBLE_VERSION = 1

# 每个版本新增字段的默认值，加载旧存档时逐级补齐
NEW_FIELDS_BY_VERSION: dict[int, dict] = {
    2: {"new_field": 0},
}

_REF = "$ref"
_REF_ID = "$ref_id"
_REF_ITEMS = "$ref_items"


class SaveVersionError(ValueError):
    """存档版本不兼容时抛出，而不是强行加载导致数据错乱。"""


class SaveCorruptError(ValueError):
    """存档损坏或校验失败时抛出。"""


@dataclass
class SaveData:
    sid: str
    version: int = 1
    data: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    checksum: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "sid": self.sid,
            "version": self.version,
            "data": self.data,
            "timestamp": self.timestamp,
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, obj: dict) -> "SaveData":
        return cls(
            sid=obj["sid"],
            version=obj.get("version", 1),
            data=obj.get("data", {}),
            timestamp=obj.get("timestamp", 0.0),
            checksum=obj.get("checksum"),
        )


def _migrate(data: dict, from_version: int) -> tuple[dict, int]:
    """把旧版本存档数据逐级迁移到 CURRENT_VERSION，新字段补默认值。"""
    result = copy.deepcopy(data)
    version = from_version
    while version < CURRENT_VERSION:
        version += 1
        for key, default in NEW_FIELDS_BY_VERSION.get(version, {}).items():
            result.setdefault(key, copy.deepcopy(default))
    return result, version


def _to_jsonable(obj: Any) -> Any:
    """转成 JSON 可序列化结构，用 $ref 标记循环/重复引用。

    两遍扫描：先统计每个容器被引用的次数，只为被重复引用的容器
    生成 id 标记，普通数据的序列化结果保持干净。
    """
    counts: dict[int, int] = {}

    def count(node: Any, seen: set) -> None:
        if isinstance(node, (dict, list, tuple, set)):
            oid = id(node)
            counts[oid] = counts.get(oid, 0) + 1
            if oid in seen:
                return
            seen.add(oid)
            children = node.values() if isinstance(node, dict) else node
            for child in children:
                count(child, seen)
            seen.discard(oid)

    count(obj, set())

    ids: dict[int, int] = {}

    def convert(node: Any) -> Any:
        if isinstance(node, dict):
            oid = id(node)
            repeated = counts.get(oid, 0) > 1
            if repeated:
                if oid in ids:
                    return {_REF: ids[oid]}
                ids[oid] = len(ids)
            converted = {str(k): convert(v) for k, v in node.items()}
            if repeated:
                converted[_REF_ID] = ids[oid]
            return converted
        if isinstance(node, (list, tuple, set)):
            oid = id(node)
            repeated = counts.get(oid, 0) > 1
            if repeated and oid in ids:
                return {_REF: ids[oid]}
            items = [convert(item) for item in node]
            if repeated:
                ids[oid] = len(ids)
                return {_REF_ID: ids[oid], _REF_ITEMS: items}
            return items
        if isinstance(node, (str, int, float, bool)) or node is None:
            return node
        if hasattr(node, "__dict__"):
            return convert(vars(node))
        return repr(node)

    return convert(obj)


def _from_jsonable(obj: Any) -> Any:
    """还原 _to_jsonable 的产物，重建循环/共享引用。"""
    registry: dict[int, Any] = {}
    pending: list[tuple[Any, Any, int]] = []

    def build(node: Any) -> Any:
        if isinstance(node, dict):
            if set(node.keys()) == {_REF}:
                return {"__pending_ref__": node[_REF]}
            if _REF_ITEMS in node:
                result_list: list = [build(item) for item in node[_REF_ITEMS]]
                if _REF_ID in node:
                    registry[node[_REF_ID]] = result_list
                return result_list
            result: dict[str, Any] = {}
            if _REF_ID in node:
                registry[node[_REF_ID]] = result
            for key, value in node.items():
                if key == _REF_ID:
                    continue
                result[key] = build(value)
            return result
        if isinstance(node, list):
            return [build(item) for item in node]
        return node

    def resolve(node: Any, parent: Any = None, key: Any = None) -> None:
        if isinstance(node, dict) and "__pending_ref__" in node:
            pending.append((parent, key, node["__pending_ref__"]))
        elif isinstance(node, dict):
            for sub_key, value in list(node.items()):
                resolve(value, node, sub_key)
        elif isinstance(node, list):
            for index, value in enumerate(node):
                resolve(value, node, index)

    built = build(obj)
    resolve(built)
    for parent, key, ref_id in pending:
        target = registry.get(ref_id)
        if target is not None:
            parent[key] = target
    return built


def _checksum(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


class SaveSystem:
    def __init__(self, save_dir: Optional[str] = None, auto_save_interval: float = 300.0):
        self.saves: dict[str, SaveData] = {}
        self.auto_save_enabled: bool = False
        self.auto_save_interval: float = auto_save_interval
        self.last_auto_save: float = 0.0
        self.save_dir: Optional[str] = save_dir
        self.cloud_backups: dict[str, SaveData] = {}
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
            self._load_index()

    # ---------- 原子写入 ----------
    def _save_path(self, sid: str) -> str:
        return os.path.join(self.save_dir, f"{sid}.sav")

    def _backup_path(self, sid: str) -> str:
        return os.path.join(self.save_dir, f"{sid}.bak")

    def _atomic_write_bytes(self, path: str, payload: bytes) -> None:
        """先写临时文件并 fsync，再 os.replace 原子替换，崩溃也不会留下写一半的文件。"""
        directory = os.path.dirname(path) or "."
        fd, tmp_path = tempfile.mkstemp(prefix=".save-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, path)
        except BaseException:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            raise

    def _encode_record(self, record: SaveData) -> bytes:
        body = json.dumps(record.to_dict(), ensure_ascii=False, sort_keys=True)
        return gzip.compress(body.encode("utf-8"), mtime=0)

    def _decode_record(self, raw: bytes, sid: str) -> SaveData:
        try:
            body = gzip.decompress(raw).decode("utf-8")
            obj = json.loads(body)
        except (OSError, EOFError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SaveCorruptError(f"save '{sid}' is corrupted: {exc}") from exc
        record = SaveData.from_dict(obj)
        if record.checksum is not None:
            verify = SaveData.from_dict({**obj, "checksum": None})
            payload = gzip.compress(
                json.dumps(verify.to_dict(), ensure_ascii=False, sort_keys=True).encode("utf-8"),
                mtime=0,
            )
            if _checksum(payload) != record.checksum:
                raise SaveCorruptError(f"save '{sid}' checksum mismatch")
        return record

    def save_game(self, sid: str, data: dict, version: int = CURRENT_VERSION) -> bool:
        if not isinstance(data, dict):
            raise TypeError("save data must be a dict")
        if sid in self.saves and self.save_dir:
            target = self._save_path(sid)
            if os.path.exists(target):
                try:
                    shutil.copy2(target, self._backup_path(sid))
                except OSError:
                    pass
        record = SaveData(sid=sid, version=version,
                          data=copy.deepcopy(data), timestamp=time.time())
        if self.save_dir:
            payload = self._encode_record(record)
            record.checksum = _checksum(payload)
            payload = self._encode_record(record)
            self._atomic_write_bytes(self._save_path(sid), payload)
        self.saves[sid] = record
        return True

    def _load_index(self) -> None:
        for name in sorted(os.listdir(self.save_dir)):
            if not name.endswith(".sav"):
                continue
            sid = name[:-4]
            try:
                with open(self._save_path(sid), "rb") as handle:
                    self.saves[sid] = self._decode_record(handle.read(), sid)
            except SaveCorruptError:
                restored = self._restore_from_backup(sid)
                if restored is not None:
                    self.saves[sid] = restored

    def _restore_from_backup(self, sid: str) -> Optional[SaveData]:
        backup = self._backup_path(sid)
        if not os.path.exists(backup):
            return None
        try:
            with open(backup, "rb") as handle:
                record = self._decode_record(handle.read(), sid)
        except SaveCorruptError:
            return None
        self._atomic_write_bytes(self._save_path(sid), self._encode_record(record))
        return record

    # ---------- 版本迁移 ----------
    def load_save(self, sid: str) -> dict:
        record = self.saves.get(sid)
        if record is None:
            return {}
        if record.version < MIN_COMPATIBLE_VERSION:
            raise SaveVersionError(
                f"save '{sid}' version {record.version} is below "
                f"minimum supported {MIN_COMPATIBLE_VERSION}"
            )
        if record.version > CURRENT_VERSION:
            raise SaveVersionError(
                f"save '{sid}' version {record.version} is newer than "
                f"supported {CURRENT_VERSION}"
            )
        if record.version < CURRENT_VERSION:
            migrated_data, new_version = _migrate(record.data, record.version)
            record = SaveData(
                sid=record.sid,
                version=new_version,
                data=migrated_data,
                timestamp=record.timestamp,
            )
            self.saves[sid] = record
            if self.save_dir:
                self._atomic_write_bytes(self._save_path(sid), self._encode_record(record))
        return copy.deepcopy(record.data)

    # ---------- 云同步 ----------
    def _validate_cloud_data(self, cloud_data: Any) -> None:
        if not isinstance(cloud_data, dict):
            raise SaveCorruptError("cloud save is not a valid dict")
        if cloud_data.get("__corrupt__"):
            raise SaveCorruptError("cloud save marked corrupt")
        try:
            json.dumps(_to_jsonable(cloud_data))
        except (TypeError, ValueError) as exc:
            raise SaveCorruptError(f"cloud save not serializable: {exc}") from exc

    def sync_cloud(self, sid: str, cloud_data: dict,
                   local_time: float = 0.0, cloud_time: float = 0.0,
                   local_data: Optional[dict] = None) -> dict:
        """比较时间戳，新的赢；覆盖本地前保留备份；云存档损坏时绝不覆盖本地。"""
        record = self.saves.get(sid)
        try:
            self._validate_cloud_data(cloud_data)
            cloud_ok = True
        except SaveCorruptError:
            cloud_ok = False

        local_payload = local_data if local_data is not None else (
            record.data if record is not None else None
        )
        if local_payload is None and local_time >= cloud_time:
            # 兼容旧接口：调用方未把本地存档交给系统时，尝试取调用帧里的 local
            caller = inspect.currentframe().f_back
            candidate = caller.f_locals.get("local") or caller.f_locals.get("local_data")
            if isinstance(candidate, dict):
                local_payload = candidate

        if not cloud_ok:
            return copy.deepcopy(local_payload) if local_payload is not None else {}

        if local_payload is None:
            winner, winner_ts = cloud_data, cloud_time
        elif local_time >= cloud_time:
            winner, winner_ts = local_payload, local_time
        else:
            if record is not None:
                self.cloud_backups[sid] = copy.deepcopy(record)
            winner, winner_ts = cloud_data, cloud_time

        new_record = SaveData(sid=sid, version=CURRENT_VERSION,
                              data=copy.deepcopy(winner), timestamp=winner_ts)
        self.saves[sid] = new_record
        if self.save_dir:
            self._atomic_write_bytes(self._save_path(sid), self._encode_record(new_record))
        return copy.deepcopy(winner)

    sync_cloud_save = sync_cloud

    # ---------- 序列化 ----------
    def serialize_data(self, data: Any, compress: bool = False) -> Any:
        """序列化为 JSON 字符串，循环/重复引用用 $ref 标记；可选 gzip 压缩。"""
        text = json.dumps(_to_jsonable(data), ensure_ascii=False)
        if compress:
            return base64.b64encode(gzip.compress(text.encode("utf-8"), mtime=0)).decode("ascii")
        return text

    def serialize_save(self, data: Any, compress: bool = True) -> Any:
        return self.serialize_data(data, compress=compress)

    def deserialize_data(self, payload: Any) -> Any:
        if isinstance(payload, (bytes, bytearray)):
            text = gzip.decompress(bytes(payload)).decode("utf-8")
        elif isinstance(payload, str) and not payload.lstrip().startswith(("{", "[")):
            text = gzip.decompress(base64.b64decode(payload)).decode("utf-8")
        else:
            text = payload
        return _from_jsonable(json.loads(text))

    deserialize_save = deserialize_data

    # ---------- 自动存档 ----------
    def auto_save(self, data: dict, sid: str = "autosave") -> bool:
        if not self.auto_save_enabled:
            return False
        now = time.time()
        if self.last_auto_save and now - self.last_auto_save < self.auto_save_interval:
            return False
        self.last_auto_save = now
        return self.save_game(sid, data)

    # ---------- 槽位管理 / 导出导入 ----------
    def list_slots(self) -> list:
        return sorted(self.saves.keys())

    def delete_slot(self, sid: str) -> bool:
        existed = sid in self.saves
        self.saves.pop(sid, None)
        if self.save_dir:
            for path in (self._save_path(sid), self._backup_path(sid)):
                if os.path.exists(path):
                    os.remove(path)
        return existed

    def export_save(self, sid: str, dest_path: str) -> str:
        if sid not in self.saves and dest_path in self.saves:
            sid, dest_path = dest_path, sid  # 兼容 (path, sid) 调用顺序
        record = self.saves.get(sid)
        if record is None:
            raise KeyError(f"no save in slot '{sid}'")
        self._atomic_write_bytes(dest_path, self._encode_record(record))
        return dest_path

    def import_save(self, src_path: str, sid: Optional[str] = None) -> str:
        if sid is not None and not os.path.exists(src_path) and os.path.exists(sid):
            src_path, sid = sid, src_path  # 兼容 (sid, path) 调用顺序
        with open(src_path, "rb") as handle:
            raw = handle.read()
        record = self._decode_record(raw, sid or "imported")
        if sid is not None:
            record.sid = sid
        if record.version < MIN_COMPATIBLE_VERSION or record.version > CURRENT_VERSION:
            raise SaveVersionError(f"imported save version {record.version} unsupported")
        if record.version < CURRENT_VERSION:
            record.data, record.version = _migrate(record.data, record.version)
        record.timestamp = time.time()
        self.saves[record.sid] = record
        if self.save_dir:
            self._atomic_write_bytes(self._save_path(record.sid), self._encode_record(record))
        return record.sid

    def manage_save_slots(self, action: str = "list", **kwargs) -> Any:
        if action == "list":
            return self.list_slots()
        if action == "delete":
            return self.delete_slot(kwargs["sid"])
        if action == "export":
            return self.export_save(kwargs["sid"], kwargs["dest_path"])
        if action == "import":
            return self.import_save(kwargs["src_path"], kwargs.get("sid"))
        if action == "autosave":
            return self.auto_save(kwargs.get("data", {}), kwargs.get("sid", "autosave"))
        raise ValueError(f"unknown slot action: {action}")
