from typing import List, Optional, Dict, Any
import requests
import json
import datetime
import os
from dataclasses import dataclass
import uuid


@dataclass(frozen=True)
class DataObjectFetcher:
    data: List[Dict[str, Any]]
    source: str
    batch_id: str
    load_timestamp: str
    filename: Optional[str] = None
    full_path: Optional[str] = None


class DataFetcher:
    def __init__(self, crypto: List[str], currency: str, url: str) -> None:
        self.crypto = crypto
        self.currency = currency
        self.url = url

    def build_url(self,
                  order: str,
                  per_page: int,
                  page: int,
                  sparkline: bool) -> str:

        ids = ",".join(self.crypto)
        return (f"{self.url}?vs_currency={self.currency}&ids={ids}"
                f"&order={order}&per_page={per_page}&page={page}"
                f"&sparkline={str(sparkline).lower()}")

    def validate_data(self, data: List[Dict[str, Any]]) -> None:
        required_fields = ['id', 'symbol', 'name', 'image', 'current_price',
                           'market_cap', 'market_cap_rank',
                           'fully_diluted_valuation', 'total_volume',
                           'high_24h', 'low_24h', 'price_change_24h',
                           'price_change_percentage_24h',
                           'market_cap_change_24h',
                           'market_cap_change_percentage_24h',
                           'circulating_supply', 'total_supply', 'max_supply',
                           'ath', 'ath_change_percentage', 'ath_date', 'atl',
                           'atl_change_percentage', 'atl_date', 'roi',
                           'last_updated']

        if not isinstance(data, list):
            raise ValueError("Object is not a list ")

        for item in data:
            for field in required_fields:
                if field not in item:
                    raise ValueError(f"Missing required field '{field}'"
                                     f" in record {item.get('id', 'UNKNOWN')}")

    def get_data(self,
                 order: str = 'market_cap_desc',
                 per_page: int = 3,
                 page: int = 1,
                 sparkline: bool = False) -> DataObjectFetcher:

        final_url = self.build_url(order, per_page, page, sparkline)

        try:
            response = requests.get(final_url)
            response.raise_for_status()
            data = response.json()
            self.validate_data(data)
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            batch = str(uuid.uuid4())
            return DataObjectFetcher(data=data,
                                     source='coingecko',
                                     filename=None,
                                     full_path=None,
                                     batch_id=batch,
                                     load_timestamp=timestamp)
        except Exception as e:
            raise RuntimeError(f"Error fetching data: {e}")

    def save_data(self,
                  data: DataObjectFetcher,
                  file_path: str) -> DataObjectFetcher:

        if data.filename is not None or data.full_path is not None:
            raise ValueError(
                "DataObjectFetcher already has filename/full_path set. "
                "Refusing to overwrite existing RAW batch."
                )

        file_name = (f"coingecko_crypto_market_{data.load_timestamp}"
                     f"_{data.batch_id}.json")

        full_path = os.path.join(file_path, file_name)

        try:
            with open(full_path, 'w') as f:
                json.dump(data.data, f, ensure_ascii=True, indent=4)
        except OSError as e:
            raise OSError(f"Failed to persist RAW data: {e}") from e

        return DataObjectFetcher(data=data.data,
                                 source=data.source,
                                 filename=file_name,
                                 full_path=full_path,
                                 batch_id=data.batch_id,
                                 load_timestamp=data.load_timestamp)
