from etl.fetcher import DataFetcher, DataObjectFetcher
from etl.cleaner import DataCleaner, DataObjectCleaner
from etl.directory_manager import DirectoryManager, ResultObjectInfrastructure
from etl.pyspark_builder import SparkDataset


class ETLPipeline:
    def __init__(self, config: dict):
        self.api_conf = config['api']
        self.paths_conf = config['paths']

        # Inicjalizacja klas
        self.directory_manager = DirectoryManager(
            raw=self.paths_conf['raw'],
            processed=self.paths_conf['processed'],
            output=self.paths_conf['output'],
            logs=self.paths_conf['logs'],
            tests=self.paths_conf['tests']
        )

        self.data_fetcher = DataFetcher(
            crypto=self.api_conf['crypto'],
            currency=self.api_conf['currency'],
            url=self.api_conf['endpoint']
        )

        self.data_cleaner = DataCleaner()
        self.spark_builder = SparkDataset()

    def ensure_dirs(self) -> ResultObjectInfrastructure:
        """Tworzenie folderów jeśli nie istnieją"""
        dirs = self.directory_manager.ensure_directory()
        return dirs

    def fetch(self) -> DataObjectFetcher:
        """Pobranie danych z API i zapis w folderze raw"""
        raw = self.data_fetcher.get_data()
        raw = self.data_fetcher.save_data(raw, self.paths_conf['raw'])
        return raw

    def clean(self, raw_obj: DataObjectFetcher) -> DataObjectCleaner:
        """Normalizacja danych z raw do processed"""
        data_obj = self.data_cleaner.load_raw(raw_obj)
        normalised = self.data_cleaner.normalize(data_obj)
        processed = self.data_cleaner.save_data(
         normalised, self.paths_conf['processed']
        )
        return processed

    def spark_transform(self, processed_obj: DataObjectCleaner) -> 'DataFrame':
        """Ładowanie przetworzonych danych do PySpark i
        zapis do output (Parquet/Delta)"""
        df = self.spark_builder.read(processed_obj)
        self.spark_builder.save(
            df, self.paths_conf['output'], processed_obj.batch_id
        )
        return df
