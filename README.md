# ETL API → PySpark DataFrame Project

## 1. Cel projektu

Celem projektu jest stworzenie modularnego procesu ETL (Extract → Transform → Load), który:

1. Pobiera dane z publicznego API.
2. Czyści i przetwarza dane w Pythonie.
3. Tworzy DataFrame w **PySparku** do dalszego wykorzystania w systemach Big Data (np. DataBricks).
4. Zapisuje dane w uporządkowanej strukturze folderów.

Projekt służy również jako **warsztat programistyczny**, rozwijający umiejętności w Pythonie, PySparku, pracy z API i ETL.

---

## 2. Funkcjonalności

* **Konfiguracja projektu**

  * Plik `config.yaml` przechowujący endpointy API, parametry zapytań i ustawienia folderów.
* **Zarządzanie folderami**

  * `DirectoryManager` do automatycznego tworzenia i organizacji folderów na dane, logi i wyniki.
* **Pobieranie danych**

  * Pobieranie JSON z API przy użyciu `requests`.
  * Obsługa błędów i retry.
  * Opcjonalnie: wprowadzenie wielowątkowości / async dla szybszego pobierania dużych zbiorów danych.
* **Czyszczenie i przetwarzanie**

  * Normalizacja JSON do struktury listy słowników.
  * Filtrowanie, walidacja i transformacja danych.
  * Obsługa brakujących wartości.
* **Tworzenie DataFrame w PySparku**

  * Inicjalizacja `SparkSession`.
  * Tworzenie DataFrame z przetworzonych danych.
  * Operacje transformacyjne: filtrowanie, grupowanie, agregacje, tworzenie kolumn pochodnych.
* **Eksport danych**

  * Zapis DataFrame w formacie CSV lub Parquet.
  * Struktura folderów uporządkowana przez `DirectoryManager`.
* **Dodatki**

  * Logowanie przebiegu ETL.
  * Metryki jakości danych (np. liczba pustych wierszy, duplikaty).
  * Przygotowanie do testów jednostkowych.

---

## 3. Struktura projektu

```
etl_project/
│
├── config/
│   └── config.yaml           # konfiguracja API i parametrów ETL
│
├── data/
│   ├── raw/                  # surowe dane pobrane z API
│   ├── processed/            # dane po czyszczeniu i transformacjach
│   └── output/               # finalny DataFrame zapisany jako CSV/Parquet
│
├── etl/
│   ├── __init__.py
│   ├── directory_manager.py  # klasa do zarządzania folderami
│   ├── fetcher.py            # moduł pobierający dane z API
│   ├── cleaner.py            # moduł czyszczący i transformujący dane
│   └── pyspark_builder.py    # moduł tworzący DataFrame w PySparku
│
├── logs/                     # logi przebiegu ETL
│
├── tests/                    # testy jednostkowe
│
├── main.py                   # główny skrypt ETL
└── README.md
```

---

## 4. Technologie i narzędzia

* **Python 3.10+**
* **PySpark** – tworzenie i transformacja DataFrame
* **Requests** – pobieranie danych z API
* **PyYAML** – obsługa konfiguracji w YAML
* **Logging** – monitorowanie przebiegu ETL
* **Threading / Async** (opcjonalnie) – przyspieszenie pobierania danych
* **Pandas** (opcjonalnie) – szybkie manipulacje przy testach przed Sparkiem
* **Parquet / CSV** – eksport danych

---

## 5. Tematy i “klocki” do dalszego rozwoju

1. **API + Requests**

   * Pobieranie danych z REST API
   * Retry i obsługa błędów
   * Multi-threaded / async fetching

2. **Konfiguracja**

   * YAML config
   * Parametryzacja endpointów, folderów i filtrów

3. **Zarządzanie folderami**

   * DirectoryManager
   * Tworzenie hierarchii folderów
   * Organizacja danych raw / processed / output

4. **Czyszczenie danych**

   * Normalizacja JSON → lista słowników
   * Walidacja danych
   * Obsługa braków i filtracja kolumn

5. **PySpark**

   * SparkSession
   * DataFrame creation
   * Transformacje: filtrowanie, agregacje, nowe kolumny
   * Zapis do CSV/Parquet

6. **ETL**

   * Modularny pipeline: Extract → Transform → Load
   * Logowanie etapów
   * Metryki jakości danych

7. **Testowanie**

   * Unit testy dla poszczególnych modułów
   * Mockowanie API
   * Testy jakości danych

---

## 6. Jak używać projektu

1. Skonfiguruj plik `config/config.yaml` z parametrami API.
2. Uruchom `main.py` – proces ETL pobierze dane, przetworzy je i zapisze w `data/output/`.
3. Wyniki możesz odczytać bezpośrednio w PySparku lub przygotować do załadowania do DataBricks.

---
