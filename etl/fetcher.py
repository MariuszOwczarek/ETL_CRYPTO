from typing import List, Optional, Dict, Any
import requests
import json
import datetime
import os

class DataFetcher:
    def __init__(self, crypto: List, currency: str, url: str) -> None:
        self.crypto = crypto
        self.currency = currency
        self.url = url

    def build_url(self, order: str, per_page: int, page: int, sparkline:bool) -> str:
        ids = ",".join(self.crypto)
        return (f"{self.url}?vs_currency={self.currency}&ids={ids}"
            f"&order={order}&per_page={per_page}&page={page}&sparkline={str(sparkline).lower()}")

    def get_data(self,
                 order: str = 'market_cap_desc',
                 per_page: int = 3,
                 page: int = 1,
                 sparkline: bool = False ) -> Optional[List[Dict[str, Any]]]:

        final_url = self.build_url(order, per_page, page, sparkline)
        try:
            response = requests.get(final_url)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def create_filename(self):
        file_name = f"coingecko_crypto_market_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.json"
        return file_name

    def save_data(self, data, file_path, file_name):
        file_path_name = os.path.join(file_path, file_name)
        with open(file_path_name, 'w') as f:
            json.dump(data, f, ensure_ascii=True, indent=4)
