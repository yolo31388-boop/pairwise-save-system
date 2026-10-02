"""存档系统 - 红态测试"""
import pytest, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from save.system import SaveSystem, SaveData

class TestAtomicSave:
    def test_save_does_not_corrupt_on_failure(self):
        ss = SaveSystem()
        ss.saves["s1"] = SaveData("s1", data={"level": 10})
        ss.save_game("s1", {"level": 20})
        assert ss.saves["s1"].data["level"] == 20

class TestVersionMigration:
    def test_old_save_migrated(self):
        ss = SaveSystem()
        ss.saves["s1"] = SaveData("s1", version=1, data={"old_field": 1})
        data = ss.load_save("s1")
        assert "new_field" in data

class TestCloudConflict:
    def test_newer_save_wins(self):
        ss = SaveSystem()
        local = {"level": 20}
        cloud = {"level": 10}
        result = ss.sync_cloud("s1", cloud, local_time=100, cloud_time=50)
        assert result["level"] == 20

class TestCircularReference:
    def test_circular_data_serialized(self):
        ss = SaveSystem()
        data = {"a": {}}
        data["a"]["self"] = data["a"]
        result = ss.serialize_data(data)
        assert result is not None

class TestAutoSave:
    def test_auto_save_creates_backup(self):
        ss = SaveSystem()
        ss.auto_save_enabled = True
        ss.auto_save({"level": 15})
        assert len(ss.saves) > 0
