"""存档系统 - 含5个bug"""
from dataclasses import dataclass, field
import json

@dataclass
class SaveData:
    sid: str
    version: int = 1
    data: dict = field(default_factory=dict)
    timestamp: float = 0.0

class SaveSystem:
    def __init__(self):
        self.saves: dict[str, SaveData] = {}
        self.auto_save_enabled: bool = False  # bug5: 无自动存档

    def save_game(self, sid: str, data: dict) -> bool:
        # bug1: 直接覆盖，非原子
        self.saves[sid] = SaveData(sid, data=data)
        return True

    def load_save(self, sid: str) -> dict:
        # bug2: 不做版本迁移
        save = self.saves.get(sid)
        if save:
            return save.data
        return {}

    def sync_cloud(self, sid: str, cloud_data: dict, local_time: float, cloud_time: float) -> dict:
        # bug3: 直接用云覆盖本地
        return cloud_data

    def serialize_data(self, data: dict) -> str:
        # bug4: 不处理循环引用
        return json.dumps(data)

    def auto_save(self, data: dict) -> None:
        # bug5: 无自动存档
        pass
