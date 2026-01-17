import os
from etl.fetcher import DataFetcher
from etl.cleaner import DataCleaner
from etl.directory_manager import DirectoryManager
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

        self.data_cleaner = DataCleaner(self.paths_conf['raw'])
        self.spark_builder = SparkDataset()

    def ensure_dirs(self):
        """Tworzenie folderów jeśli nie istnieją"""
        self.directory_manager.ensure_directory()

    def fetch(self):
        """Pobranie danych z API i zapis w folderze raw"""
        data = self.data_fetcher.get_data()
        filename = self.data_fetcher.create_filename()
        self.data_fetcher.save_data(data, self.paths_conf['raw'], filename)
        return filename

    def clean(self, filename: str):
        """Normalizacja danych z raw do processed"""
        raw_file = self.data_cleaner.load_raw()
        processed_data = self.data_cleaner.normalize(raw_file, self.paths_conf['processed'])
        self.data_cleaner.save_data(processed_data)
        return processed_data

    def spark_transform(self):
        """Ładowanie przetworzonych danych do PySpark i zapis do output (Parquet/Delta)"""
        path_processed = os.path.join(self.paths_conf['processed'], "*.json")
        df = self.spark_builder.read(path_processed)
        self.spark_builder.save(df, self.paths_conf['output'])
        return df

    def preview_parquet(self, n=5):
        """Podejrzenie kilku wierszy z Parquet"""
        df = self.spark_builder.read_parquet(os.path.join(self.paths_conf['output'], "*.parquet"))
        df.show(n)
