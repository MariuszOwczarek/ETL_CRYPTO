import os
import logging
from enum import Enum, auto
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class Status(Enum):
    SUCCESS = auto()
    FAILURE = auto()


class Event(Enum):
    DIRECTORY_CREATION = auto()


@dataclass(frozen=True)
class ResultObjectInfrastructure():
    status: Enum
    event: Enum
    folder: str | None
    error: str | None

class DirectoryManager:
    def __init__(self, raw, processed, output, logs, tests):
        self.raw = raw
        self.processed = processed
        self.output = output
        self.logs = logs
        self.tests = tests

    def ensure_directory(self):
        folders = [self.raw,
                   self.processed,
                   self.output,
                   self.logs,
                   self.tests]

        for folder in folders:
            available = os.path.exists(folder)
            if not available:
                try:
                    os.makedirs(folder, exist_ok=True)
                except Exception as e:
                    logger.error(f"Folder Not Created {folder}")
                    return ResultObjectInfrastructure(
                            status=Status.FAILURE,
                            event=Event.DIRECTORY_CREATION,
                            folder=folder,
                            error=str(e))
        return ResultObjectInfrastructure(
                            status=Status.SUCCESS,
                            event=Event.DIRECTORY_CREATION,
                            folder=None,
                            error=None)
