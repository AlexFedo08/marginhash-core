class AggregationPipeline:
    """
    Система управления потоками данных (Data Pipeline).
    Отвечает за последовательность агрегации, дедупликации и обновления данных.
    """

    def __init__(self, storage_client):
        self.storage = storage_client
        self.normalizer = DeviceNormalizer()

    async def run_sync(self):
        """Запуск цикла синхронизации данных из внешних узлов."""
        raw_payloads = await self._fetch_distributed_data()
        normalized_data = [self.normalizer.transform_to_schema(item) for item in raw_payloads]
        
        # Логика дедупликации и слияния цен
        final_catalog = self._merge_provider_offers(normalized_data)
        
        self.storage.update_catalog(final_catalog)

    def _merge_provider_offers(self, data):
        """Алгоритм выбора лучшего предложения для каждой модели."""
        return data
