import datetime
import json
import logging
import os
from dataclasses import dataclass
from glob import glob
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataObject:
    data: List[Dict[str, Any]]
    filename: str
    full_path: str
    load_timestamp: str


class DataCleaner:
    def __init__(self, file_path):
        self.file_path = file_path

    def load_raw(self) -> Optional[DataObject]:
        file_list = sorted([f for f in glob(f"{self.file_path}/*.json")])
        if file_list:
            try:
                with open(file_list[0], 'r') as f:
                    return DataObject(
                        data=json.load(f),
                        filename=os.path.basename(file_list[0]),
                        full_path=os.path.join(self.file_path, os.path.basename(file_list[0])),
                        load_timestamp=datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    )
            except (FileNotFoundError, json.JSONDecodeError) as e:
                logger.error(e)
                return None
        else:
            raise FileNotFoundError("No JSON files found in RAW folder")

    def normalize(self, data_raw, processed_path) -> DataObject:
        data_processed = []
        for crypto in data_raw.data:
            record_processed = {}
            for key in crypto:
                value = crypto[key]
                if any(isinstance(value, t) for t in [int,
                                                      float,
                                                      str,
                                                      bool,
                                                      type(None)]):
                    record_processed[key] = value
                elif any(isinstance(value, t) for t in [dict, list]):
                    if key == 'roi':
                        if value is not None:
                            for subkey, subvalue in value.items():
                                record_processed[f'roi_{subkey}'] = subvalue
                        else:
                            record_processed['roi_times'] = None
                            record_processed['roi_currency'] = None
                            record_processed['roi_percentage'] = None
            data_processed.append(record_processed)
        return DataObject(data=data_processed,
                          filename=data_raw.filename,
                          full_path=os.path.join(processed_path, data_raw.filename),
                          load_timestamp=data_raw.load_timestamp)

    def save_data(self, data):
        with open(data.full_path, 'w') as f:
            json.dump(data.data, f, ensure_ascii=True, indent=4)
