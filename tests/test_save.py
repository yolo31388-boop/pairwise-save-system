"""存档系统测试 - 覆盖原子写入/版本迁移/云同步/序列化/槽位管理"""
import gzip
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from save.system import (
    AUTO_SAVE_SLOT,
    CURRENT_VERSION,
    CorruptSaveError,
    IncompatibleVersionError,
    SaveData,
    SaveSystem,
)


@pytest.fixture
def ss(tmp_path):
    return SaveSystem(save_dir=tmp_path / "saves")


class TestAtomicSave:
    def test_save_and_load_roundtrip(self, ss):
        assert ss.save_game("s1", {"level": 20})
        assert ss.load_save("s1")["level"] == 20
        assert ss.saves["s1"].version == CURRENT_VERSION

    def test_failed_save_keeps_old_file(self, ss, monkeypatch):
        ss.save_game("s1", {"level": 10})

        def boom(src, dst):
            raise OSError("模拟崩溃/断电")

        monkeypatch.setattr("save.system.os.replace", boom)
        with pytest.raises(OSError):
            ss.save_game("s2", {"level": 99})
        monkeypatch.undo()
        assert ss.load_save("s2") == {}  # 写一半的文件不存在
        assert not list(ss.save_dir.glob("*.tmp"))  # 临时文件已清理
        assert ss.load_save("s1")["level"] == 10  # 旧存档完好

    def test_backup_created_and_restorable(self, ss):
        ss.save_game("s1", {"level": 1})
        ss.save_game("s1", {"level": 2})
        backup = ss.save_dir / "s1.sav.bak"
        assert backup.exists()
        (ss.save_dir / "s1.sav").write_bytes(b"garbage")  # 模拟损坏
        with pytest.raises(CorruptSaveError):
            ss.load_save("s1")
        assert ss.restore_backup("s1")["level"] == 1


class TestVersionMigration:
    def test_old_save_migrated(self, ss):
        ss.saves["s1"] = SaveData("s1", version=1, data={"old_field": 1})
        data = ss.load_save("s1")
        assert data["old_field"] == 1  # 旧数据保留
        assert "new_field" in data  # v2 新字段补默认值
        assert "playtime_hours" in data  # 逐级迁移到 v3
        assert ss.saves["s1"].version == CURRENT_VERSION

    def test_future_version_rejected(self, ss):
        ss.write_envelope("s1", {"level": 1}, version=CURRENT_VERSION + 1)
        with pytest.raises(IncompatibleVersionError):
            ss.load_save("s1")


class TestCloudConflict:
    def test_newer_local_save_kept(self, ss):
        ss.saves["s1"] = SaveData("s1", data={"level": 20})
        result = ss.sync_cloud("s1", {"level": 10}, local_time=100, cloud_time=50)
        assert result["level"] == 20  # 本地较新, 不被云端旧档覆盖

    def test_newer_cloud_wins_and_local_backed_up(self, ss):
        ss.save_game("s1", {"level": 10})
        result = ss.sync_cloud("s1", {"level": 30}, local_time=100, cloud_time=200)
        assert result["level"] == 30
        assert ss.load_save("s1")["level"] == 30
        assert (ss.save_dir / "s1.sav.bak").exists()  # 覆盖前保留本地备份

    def test_corrupt_cloud_save_rejected(self, ss):
        ss.save_game("s1", {"level": 20})
        bad_payloads = [
            {"corrupt": True, "level": 99},  # 损坏标记
            b"\x1f\x8b not really gzip",     # 伪装 gzip 的坏字节
            "not-a-save",                    # 非法类型
        ]
        for bad in bad_payloads:
            with pytest.raises(CorruptSaveError):
                ss.sync_cloud("s1", bad, local_time=100, cloud_time=200)
        assert ss.load_save("s1")["level"] == 20  # 本地好存档没被坏云档覆盖


class TestSerialization:
    def test_circular_data_serialized(self, ss):
        data = {"a": {}}
        data["a"]["self"] = data["a"]
        result = ss.serialize_data(data)
        assert result is not None

    def test_circular_data_roundtrip(self, ss):
        data = {"name": "root", "child": {"tag": "c"}}
        data["child"]["parent"] = data  # 互相引用
        data["self"] = data             # 自引用
        restored = ss.deserialize_save(ss.serialize_save(data))
        assert restored["self"] is restored
        assert restored["child"]["parent"] is restored
        assert restored["child"]["tag"] == "c"

    def test_serialized_data_is_compressed(self, ss):
        payload = {"log": "A" * 100_000}
        blob = ss.serialize_save(payload)
        assert blob[:2] == b"\x1f\x8b"  # gzip 魔数
        raw = json.dumps(payload).encode("utf-8")
        assert len(blob) < len(raw) // 10
        assert ss.deserialize_save(blob) == payload


class TestSaveSlots:
    def test_auto_save(self, ss):
        ss.auto_save({"level": 1})
        assert ss.saves == {}  # 未开启时不写
        ss.auto_save_enabled = True
        assert ss.auto_save({"level": 15})
        assert AUTO_SAVE_SLOT in ss.saves
        assert ss.load_save(AUTO_SAVE_SLOT)["level"] == 15

    def test_export_import_roundtrip(self, ss, tmp_path):
        ss.save_game("slot1", {"level": 42, "gold": 7})
        exported = ss.export_save("slot1", tmp_path / "backup" / "slot1.sav")
        assert exported.exists()
        other = SaveSystem(save_dir=tmp_path / "other")
        imported = other.import_save(exported, "slot1")
        assert imported == {"level": 42, "gold": 7}
        assert other.load_save("slot1")["gold"] == 7

    def test_import_corrupt_file_rejected(self, ss, tmp_path):
        ss.save_game("s1", {"level": 5})
        bad = tmp_path / "bad.sav"
        bad.write_bytes(b"\x00\x01 broken")
        with pytest.raises(CorruptSaveError):
            ss.import_save(bad, "s1")
        assert ss.load_save("s1")["level"] == 5  # 已有存档不受影响
