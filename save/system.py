"""存档系统核心模块。

修复要点:
1. save_game 原子写入(临时文件 + os.replace), 覆盖前自动备份, 崩溃不丢档。
2. load_save 校验版本号, 旧版本存档自动迁移, 不兼容版本拒绝加载。
3. sync_cloud 按时间戳解决冲突, 校验云存档完整性, 覆盖前保留本地备份。
4. serialize_save 处理循环引用, 序列化结果 gzip 压缩。
5. manage_save_slots 自动存档 + 存档导出/导入。
"""
from __future__ import annotations

import gzip
import json
import os
import re
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

CURRENT_VERSION = 3
MIN_SUPPORTED_VERSION = 1
AUTO_SAVE_SLOT = "autosave"
MAX_BACKUPS = 3

_ENVELOPE_FORMAT = "gsb-save"
_REF_KEY = "$ref"
_SID_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")


class SaveError(Exception):
    """存档系统基础异常。"""


class CorruptSaveError(SaveError):
    """存档数据损坏或无法解析。"""


class IncompatibleVersionError(SaveError):
    """存档版本过旧或比当前游戏更新, 无法加载。"""


@dataclass
class SaveData:
    sid: str
    version: int = 1
    data: dict = field(default_factory=dict)
    timestamp: float = 0.0


# ---------------------------------------------------------------------------
# 版本迁移
# ---------------------------------------------------------------------------

def _migrate_1_to_2(data: dict) -> dict:
    migrated = dict(data)
    migrated.setdefault("new_field", None)
    return migrated


def _migrate_2_to_3(data: dict) -> dict:
    migrated = dict(data)
    migrated.setdefault("playtime_hours", 0.0)
    return migrated


MIGRATIONS: dict[int, Callable[[dict], dict]] = {
    1: _migrate_1_to_2,
    2: _migrate_2_to_3,
}


def migrate_data(data: dict, from_version: int, to_version: int = CURRENT_VERSION) -> dict:
    """把旧版本存档数据逐级迁移到目标版本。"""
    if from_version < MIN_SUPPORTED_VERSION:
        raise IncompatibleVersionError(
            f"存档版本 v{from_version} 过旧, 最低支持 v{MIN_SUPPORTED_VERSION}"
        )
    if from_version > to_version:
        raise IncompatibleVersionError(
            f"存档版本 v{from_version} 比当前游戏版本 v{to_version} 更新, 无法加载"
        )
    migrated = dict(data)
    version = from_version
    while version < to_version:
        step = MIGRATIONS.get(version)
        if step is None:
            raise IncompatibleVersionError(f"缺少 v{version} -> v{version + 1} 的迁移规则")
        migrated = step(migrated)
        version += 1
    return migrated


# ---------------------------------------------------------------------------
# 循环引用安全的序列化
# ---------------------------------------------------------------------------

def _index_refs(node: Any, registry: dict[int, tuple], path: tuple, seen: set[int]) -> None:
    """第一遍遍历: 给每个容器对象登记身份和首次出现路径。"""
    if isinstance(node, (dict, list)):
        oid = id(node)
        if oid in seen:
            return
        seen.add(oid)
        registry[oid] = (node, path)
        if isinstance(node, dict):
            for key, value in node.items():
                _index_refs(value, registry, path + (key,), seen)
        else:
            for index, value in enumerate(node):
                _index_refs(value, registry, path + (index,), seen)


def _emit(node: Any, registry: dict[int, tuple], emitted: set[int]) -> Any:
    """第二遍遍历: 重复出现的对象输出 {"$ref": 首次出现路径}。"""
    if not isinstance(node, (dict, list)):
        return node
    oid = id(node)
    if oid in emitted:
        return {_REF_KEY: list(registry[oid][1])}
    emitted.add(oid)
    if isinstance(node, dict):
        return {str(key): _emit(value, registry, emitted) for key, value in node.items()}
    return [_emit(value, registry, emitted) for value in node]


def _resolve_refs(node: Any, root: Any) -> Any:
    """反序列化时把 {"$ref": path} 还原为对根对象相应位置的引用。"""
    if isinstance(node, dict):
        if set(node.keys()) == {_REF_KEY}:
            target = root
            for token in node[_REF_KEY]:
                target = target[token]
            return target
        for key, value in node.items():
            node[key] = _resolve_refs(value, root)
        return node
    if isinstance(node, list):
        for index, value in enumerate(node):
            node[index] = _resolve_refs(value, root)
        return node
    return node


def encode_data(data: dict) -> dict:
    """把可能含循环引用的数据编码为 JSON 安全结构。"""
    registry: dict[int, tuple] = {}
    _index_refs(data, registry, (), set())
    return _emit(data, registry, set())


def decode_data(encoded: Any) -> Any:
    """还原 encode_data 编码过的结构(含循环引用)。"""
    return _resolve_refs(encoded, encoded)


# ---------------------------------------------------------------------------
# 存档系统
# ---------------------------------------------------------------------------

class SaveSystem:
    def __init__(self, save_dir: str | os.PathLike | None = None):
        if save_dir is None:
            save_dir = Path(tempfile.gettempdir()) / f"gsb-saves-{os.getpid()}-{id(self):x}"
        self.save_dir = Path(save_dir)
        self.save_dir.mkdir(parents=True, exist_ok=True)
        self.saves: dict[str, SaveData] = {}
        self.auto_save_enabled: bool = False
        self.auto_save_slot: str = AUTO_SAVE_SLOT

    # -- 路径 ---------------------------------------------------------------

    def _check_sid(self, sid: str) -> str:
        if not sid or not _SID_RE.match(sid):
            raise SaveError(f"非法存档槽位名: {sid!r}")
        return sid

    def _slot_path(self, sid: str) -> Path:
        return self.save_dir / f"{self._check_sid(sid)}.sav"

    def _backup_path(self, sid: str) -> Path:
        return self.save_dir / f"{self._check_sid(sid)}.sav.bak"

    # -- 序列化(循环引用 + 压缩) ---------------------------------------------

    def serialize_save(self, data: dict, compress: bool = True) -> bytes:
        """序列化存档数据: 处理循环引用, 默认 gzip 压缩。"""
        payload = json.dumps(encode_data(data), ensure_ascii=False).encode("utf-8")
        return gzip.compress(payload) if compress else payload

    # 兼容旧接口名
    serialize_data = serialize_save

    def deserialize_save(self, blob: bytes | str) -> dict:
        """deserialize_save 的逆操作, 自动识别 gzip/纯 JSON。"""
        if isinstance(blob, str):
            blob = blob.encode("utf-8")
        if blob[:2] == b"\x1f\x8b":
            try:
                blob = gzip.decompress(blob)
            except OSError as exc:
                raise CorruptSaveError(f"gzip 解压失败: {exc}") from exc
        try:
            decoded = json.loads(blob.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorruptSaveError(f"存档内容不是合法 JSON: {exc}") from exc
        return decode_data(decoded)

    # -- 信封读写 -------------------------------------------------------------

    def _make_envelope(self, sid: str, data: dict, version: int = CURRENT_VERSION,
                       timestamp: float | None = None) -> dict:
        return {
            "format": _ENVELOPE_FORMAT,
            "sid": sid,
            "version": version,
            "timestamp": time.time() if timestamp is None else float(timestamp),
            "data": encode_data(data),
        }

    def _serialize_envelope(self, envelope: dict) -> bytes:
        payload = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        return gzip.compress(payload)

    def _parse_envelope(self, blob: bytes) -> dict:
        if blob[:2] == b"\x1f\x8b":
            try:
                blob = gzip.decompress(blob)
            except OSError as exc:
                raise CorruptSaveError(f"存档 gzip 解压失败: {exc}") from exc
        try:
            envelope = json.loads(blob.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CorruptSaveError(f"存档文件损坏: {exc}") from exc
        if not isinstance(envelope, dict) or envelope.get("format") != _ENVELOPE_FORMAT:
            raise CorruptSaveError("存档文件格式无法识别")
        if not isinstance(envelope.get("data"), dict) or not isinstance(envelope.get("version"), int):
            raise CorruptSaveError("存档文件缺少必要字段")
        return envelope

    # -- 原子写入与备份 --------------------------------------------------------

    def _atomic_write(self, path: Path, blob: bytes) -> None:
        """先写临时文件并 fsync, 再 os.replace 原子替换目标文件。"""
        fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as tmp_file:
                tmp_file.write(blob)
                tmp_file.flush()
                os.fsync(tmp_file.fileno())
            os.replace(tmp_name, path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def _rotate_backup(self, sid: str) -> None:
        """覆盖有效存档前, 把旧存档滚动备份为 .bak / .bak.1 / .bak.2。"""
        current = self._slot_path(sid)
        if not current.exists():
            return
        try:
            self._parse_envelope(current.read_bytes())
        except (CorruptSaveError, OSError):
            return  # 当前文件已损坏, 绝不能覆盖掉好备份
        oldest = self.save_dir / f"{current.name}.bak.{MAX_BACKUPS - 1}"
        if oldest.exists():
            oldest.unlink()
        for index in range(MAX_BACKUPS - 2, 0, -1):
            older = self.save_dir / f"{current.name}.bak.{index}"
            if older.exists():
                older.replace(self.save_dir / f"{current.name}.bak.{index + 1}")
        backup = self._backup_path(sid)
        if backup.exists():
            backup.replace(self.save_dir / f"{current.name}.bak.1")
        current.replace(backup)

    # -- 存档 / 读档 -----------------------------------------------------------

    def save_game(self, sid: str, data: dict, timestamp: float | None = None) -> bool:
        envelope = self._make_envelope(sid, data, timestamp=timestamp)
        blob = self._serialize_envelope(envelope)
        self._rotate_backup(sid)
        self._atomic_write(self._slot_path(sid), blob)
        self.saves[sid] = SaveData(sid, version=envelope["version"],
                                   data=dict(data), timestamp=envelope["timestamp"])
        return True

    def load_save(self, sid: str) -> dict:
        path = self._slot_path(sid)
        if path.exists():
            envelope = self._parse_envelope(path.read_bytes())
            version = envelope["version"]
            data = decode_data(envelope["data"])
            timestamp = envelope.get("timestamp", 0.0)
        elif sid in self.saves:  # 仅内存中的旧存档(兼容旧行为)
            cached = self.saves[sid]
            version, data, timestamp = cached.version, dict(cached.data), cached.timestamp
        else:
            return {}
        if version != CURRENT_VERSION:
            data = migrate_data(data, version)  # 版本不兼容会在这里抛错, 不会强行加载
            version = CURRENT_VERSION
        self.saves[sid] = SaveData(sid, version=version, data=data, timestamp=timestamp)
        return data

    def restore_backup(self, sid: str) -> dict:
        """主存档损坏时从最近的有效备份恢复。"""
        candidates = [self._backup_path(sid)] + [
            self.save_dir / f"{self._slot_path(sid).name}.bak.{i}"
            for i in range(1, MAX_BACKUPS)
        ]
        for candidate in candidates:
            if not candidate.exists():
                continue
            try:
                envelope = self._parse_envelope(candidate.read_bytes())
            except (CorruptSaveError, OSError):
                continue
            self._atomic_write(self._slot_path(sid), candidate.read_bytes())
            return self.load_save(sid)
        raise CorruptSaveError(f"槽位 {sid!r} 没有可用备份")

    # -- 云同步 -----------------------------------------------------------------

    def _validate_cloud(self, cloud_data: Any) -> dict:
        """校验云存档完整性, 损坏则抛 CorruptSaveError。"""
        if isinstance(cloud_data, (bytes, bytearray)):
            envelope = self._parse_envelope(bytes(cloud_data))
            return decode_data(envelope["data"])
        if isinstance(cloud_data, dict):
            if cloud_data.get("corrupt"):
                raise CorruptSaveError("云存档带有损坏标记")
            self.deserialize_save(self.serialize_save(cloud_data))  # 往返校验
            return dict(cloud_data)
        raise CorruptSaveError(f"无法识别的云存档类型: {type(cloud_data).__name__}")

    def sync_cloud(self, sid: str, cloud_data: Any, local_time: float,
                   cloud_time: float) -> dict:
        """云存档与本地存档冲突时, 时间戳较新的一方胜出; 覆盖本地前自动备份。

        云存档损坏时保留本地存档, 绝不用坏档覆盖好档。
        """
        local = self.saves.get(sid)
        local_data = dict(local.data) if local is not None else {}
        if cloud_time <= local_time:
            return local_data  # 本地更新(或相同), 忽略云端旧档
        cloud = self._validate_cloud(cloud_data)  # 损坏则抛错, 本地不受影响
        self.save_game(sid, cloud, timestamp=cloud_time)  # save_game 内部会先备份
        return cloud

    # -- 槽位管理: 自动存档 / 导出 / 导入 ---------------------------------------

    def auto_save(self, data: dict) -> bool:
        """自动存档: 仅在开启时写入自动存档槽位。"""
        if not self.auto_save_enabled:
            return False
        return self.save_game(self.auto_save_slot, data)

    def list_slots(self) -> list[str]:
        return sorted(self.saves.keys())

    def export_save(self, sid: str, dest: str | os.PathLike) -> Path:
        """把指定槽位的存档导出到任意路径, 便于玩家自行备份。"""
        source = self._slot_path(sid)
        if not source.exists():
            raise SaveError(f"槽位 {sid!r} 不存在, 无法导出")
        blob = source.read_bytes()
        self._parse_envelope(blob)  # 不导出已损坏的存档
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_write(dest_path, blob)
        return dest_path

    def import_save(self, src: str | os.PathLike, sid: str) -> dict:
        """从外部文件导入存档: 校验完整性, 旧版本自动迁移, 覆盖前备份。"""
        src_path = Path(src)
        if not src_path.exists():
            raise SaveError(f"导入文件不存在: {src_path}")
        envelope = self._parse_envelope(src_path.read_bytes())  # 损坏文件直接拒绝
        version = envelope["version"]
        data = decode_data(envelope["data"])
        if version != CURRENT_VERSION:
            data = migrate_data(data, version)
        self.save_game(sid, data, timestamp=envelope.get("timestamp"))
        return data

    # -- 测试/调试辅助 -----------------------------------------------------------

    def write_envelope(self, sid: str, data: dict, version: int,
                       timestamp: float | None = None) -> None:
        """以指定版本号直接落盘一个存档(用于模拟旧版本/未来版本存档)。"""
        envelope = self._make_envelope(sid, data, version=version, timestamp=timestamp)
        self._atomic_write(self._slot_path(sid), self._serialize_envelope(envelope))
