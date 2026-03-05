from typing import Dict, Any
import hashlib

class DeviceNormalizer:
    """
    Модуль приведения данных из различных источников к единому стандарту MarginHash.
    Обеспечивает целостность базы данных оборудования.
    """

    @staticmethod
    def generate_unique_id(source_name: str, model_name: str, hashrate: float) -> str:
        """Создание уникального хеша устройства для предотвращения дубликатов."""
        payload = f"{source_name}_{model_name}_{hashrate}"
        return hashlib.md5(payload.encode()).hexdigest()

    def transform_to_schema(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Маппинг сырых данных в стандартную схему проекта."""
        # Показываем только структуру, без логики очистки строк
        return {
            "uuid": self.generate_unique_id(raw_data.get("src"), raw_data.get("name"), raw_data.get("hr")),
            "model": raw_data.get("name", "Unknown"),
            "specs": {
                "hashrate": float(raw_data.get("hr", 0)),
                "power": int(raw_data.get("pwr", 0)),
                "efficiency": 0 # Рассчитывается далее
            },
            "market_data": {
                "price_rub": raw_data.get("price"),
                "vendor": raw_data.get("src")
            }
        }
