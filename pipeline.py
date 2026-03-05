import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MarginHash-Pipeline")

class AggregationPipeline:
    """
    High-Performance Data Pipeline Engine.
    Управляет жизненным циклом данных: от извлечения (Ingestion) 
    до финальной индексации в базе данных.
    """

    def __init__(self, storage_client: Any, concurrency_limit: int = 5):
        self.storage = storage_client
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.start_time = None
        self.stats = {"processed": 0, "errors": 0, "skipped": 0}

    async def run_sync_cycle(self):
        """
        Основной цикл синхронизации. 
        Реализует паттерн 'Extract-Transform-Load' (ETL).
        """
        self.start_time = datetime.now()
        logger.info(f"Запуск цикла агрегации: {self.start_time}")

        try:
            # Сбор данных из распределенных узлов 
            raw_data = await self._fetch_all_sources()
            
            # Валидация и очистка
            cleaned_data = self._validate_payloads(raw_data)
            
            # Трансформация и обогащение
            normalized_collection = self._transform_to_internal_schema(cleaned_data)
            
            # Дедупликация и разрешение конфликтов цен
            final_catalog = self._resolve_market_conflicts(normalized_collection)
            
            # Атомарное обновление хранилища
            await self._commit_to_storage(final_catalog)

            execution_time = datetime.now() - self.start_time
            logger.info(f"Цикл завершен успешно за {execution_time}. Индексировано: {len(final_catalog)} ед.")

        except Exception as e:
            logger.error(f"Критическая ошибка конвейера: {str(e)}")
            raise

    async def _fetch_all_sources(self) -> List[Dict]:
        """
        Асинхронное получение данных. 
        Конкретные парсеры вынесены в закрытые модули.
        """
        tasks = [self._fetch_node_data(i) for i in range(10)] 
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [res for res in results if isinstance(res, dict)]

    async def _fetch_node_data(self, node_id: int) -> Dict:
        async with self.semaphore:
            await asyncio.sleep(0.1) 
            return {"node": node_id, "data": []}

    def _validate_payloads(self, data: List[Dict]) -> List[Dict]:
        """Проверка целостности структуры данных (Schema Validation)."""
        valid_items = []
        for item in data:
            if all(k in item for k in ("model", "price", "hashrate")):
                valid_items.append(item)
            else:
                self.stats["skipped"] += 1
        return valid_items

    def _resolve_market_conflicts(self, collection: List[Dict]) -> List[Dict]:
        """
        Алгоритм разрешения конфликтов (Conflict Resolution).
        Если одна модель есть у 5 дилеров, выбирается оптимальный оффер.
        """
        merged = {}
        for entry in collection:
            model_id = entry.get("uuid")
            if model_id not in merged:
                merged[model_id] = entry
            else:
                # Логика выбора лучшей цены или более свежей даты
                merged[model_id] = self._compare_and_update(merged[model_id], entry)
        return list(merged.values())

    def _compare_and_update(self, existing: Dict, new: Dict) -> Dict:
        """Сравнение двух офферов на идентичное оборудование."""
        return new if new['market_data']['price_rub'] < existing['market_data']['price_rub'] else existing

    async def _commit_to_storage(self, data: List[Dict]):
        """Финальная запись в БД или кэш (NVMe/Redis)."""
        if not data:
            return
        await self.storage.save_batch(data)
