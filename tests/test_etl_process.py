import shutil
import pytest
import yaml
import os
from etl.fetcher import DataFetcher, DataObjectFetcher
from etl.cleaner import DataCleaner, DataObjectCleaner
from etl.directory_manager import DirectoryManager, Status
from etl.pyspark_builder import SparkDataset


# -------------------------------
# Fixtures
# -------------------------------

@pytest.fixture(scope="session")
def config():
    with open("./config/config.yaml") as f:
        cfg = yaml.safe_load(f)
    return cfg


@pytest.fixture
def data_fetcher(config):
    return DataFetcher(
        config['api']['crypto'],
        config['api']['currency'],
        config['api']['endpoint']
    )


@pytest.fixture
def dir_manager(config):
    return DirectoryManager(
            raw=config['paths']['raw'],
            processed=config['paths']['processed'],
            output=config['paths']['output'],
            logs=config['paths']['logs'],
            tests=config['paths']['tests']
            )


@pytest.fixture
def cleaner():
    return DataCleaner()


@pytest.fixture(scope="session")
def spark_dataset():
    return SparkDataset()


# -------------------------------
# Fixture z automatycznym cleanup
# -------------------------------

@pytest.fixture
def clean_directories(config):
    yield

    paths = [config['paths']['raw'],
             config['paths']['processed'],
             config['paths']['output']]

    for path in paths:
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"Usunięto folder: {path}")


# -------------------------------
# Test mini-pipeline
# -------------------------------

def test_full_pipeline(dir_manager,
                       data_fetcher,
                       cleaner,
                       spark_dataset,
                       config,
                       clean_directories):

    # 0 Katalogi: tworzenie struktury

    result = dir_manager.ensure_directory()
    assert result.status == Status.SUCCESS

    # 1️⃣ Fetcher: pobranie danych i zapis do RAW
    data = data_fetcher.get_data()
    saved_obj = data_fetcher.save_data(data, config['paths']['raw'])

    # Sprawdzenie DataObjectFetcher
    assert isinstance(saved_obj, DataObjectFetcher)
    assert saved_obj.data
    assert saved_obj.source == 'coingecko'
    assert isinstance(saved_obj.batch_id, str)
    assert isinstance(saved_obj.load_timestamp, str)
    assert os.path.exists(saved_obj.full_path)

    # 2️⃣ Cleaner: load_raw → normalize → save_data
    raw_obj = cleaner.load_raw(saved_obj)
    normalized_obj = cleaner.normalize(raw_obj)
    processed_obj = cleaner.save_data(normalized_obj,
                                      config['paths']['processed'])

    # Sprawdzenie DataObjectCleaner
    assert isinstance(processed_obj, DataObjectCleaner)
    assert processed_obj.data
    for record in processed_obj.data:
        assert 'id' in record
        assert 'symbol' in record
        assert 'name' in record
        if 'roi_times' in record:
            assert isinstance(record['roi_times'], (float, type(None)))

    # 3️⃣ SparkDataset: wczytanie PROCESSED JSON i zapis Parquet
    df = spark_dataset.read(processed_obj)  # wczytanie JSON do DataFrame
    output_path = spark_dataset.save(df,
                                     config['paths']['output'],
                                     processed_obj.batch_id)

    # Sprawdzenie, że folder batch został utworzony
    assert os.path.exists(output_path)

    # Wczytanie z powrotem DataFrame do sprawdzenia liczby rekordów
    df_check = spark_dataset.read(processed_obj)
    assert df_check.count() == len(processed_obj.data)
    record_keys = set(processed_obj.data[0].keys()) - {'roi'}
    assert set(df_check.columns) >= record_keys
