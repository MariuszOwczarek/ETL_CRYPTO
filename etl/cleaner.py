import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List
from etl.fetcher import DataObjectFetcher

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataObjectCleaner:
    data: List[Dict[str, Any]]
    filename: str
    full_path: str
    load_timestamp: str
    batch_id: str


class DataCleaner:

    def load_raw(self, raw_obj: DataObjectFetcher) -> DataObjectCleaner:
        if not raw_obj.full_path:
            raise FileNotFoundError("RAW object does not have full_path set")

        try:
            with open(raw_obj.full_path, 'r') as f:
                data = json.load(f)
            return DataObjectCleaner(
                data=data,
                filename=raw_obj.filename,
                full_path=raw_obj.full_path,
                load_timestamp=raw_obj.load_timestamp,
                batch_id=raw_obj.batch_id
                )
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load RAW data: {e}")
            raise

    def normalize(self, data_obj: DataObjectCleaner) -> DataObjectCleaner:
        normalized_data = []
        for crypto in data_obj.data:
            record = {}
            for key, value in crypto.items():
                if isinstance(value, (int, float, str, bool)) or value is None:
                    record[key] = value
                elif isinstance(value, dict) and key == 'roi':
                    record['roi_times'] = value.get('times') if value else None
                    record['roi_currency'] = value.get('currency') if value else None
                    record['roi_percentage'] = value.get('percentage') if value else None
            normalized_data.append(record)

        return DataObjectCleaner(
            data=normalized_data,
            filename=data_obj.filename,
            full_path=data_obj.full_path,
            load_timestamp=data_obj.load_timestamp,
            batch_id=data_obj.batch_id
        )

    def save_data(self, data_obj: DataObjectCleaner, processed_path: str) -> DataObjectCleaner:
        processed_file_path = os.path.join(processed_path, data_obj.filename)
        if os.path.exists(processed_file_path):
            raise FileExistsError(f"Processed file already exists: {processed_file_path}")

        with open(processed_file_path, 'w') as f:
            json.dump(data_obj.data, f, ensure_ascii=True, indent=4)

        return DataObjectCleaner(
            data=data_obj.data,
            filename=data_obj.filename,
            full_path=processed_file_path,
            load_timestamp=data_obj.load_timestamp,
            batch_id=data_obj.batch_id
            )
