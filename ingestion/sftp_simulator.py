import os
import shutil
from datetime import datetime
from framework.logger import get_logger
from utils.file_utils import check_file_exists, check_file_not_empty

logger = get_logger()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SOURCE_DIR = os.path.join(BASE_DIR, "data", "source")
LANDING_BASE_DIR = os.path.join(BASE_DIR, "data", "raw")


def create_landing_path():
    today = datetime.now()

    path = os.path.join(
        LANDING_BASE_DIR,
        str(today.year),
        str(today.month).zfill(2),
        str(today.day).zfill(2)
    )

    os.makedirs(path, exist_ok=True)
    return path


def move_files_to_landing():
    if not os.path.exists(SOURCE_DIR):
        logger.error(f"Source directory not found: {SOURCE_DIR}")
        return

    files = os.listdir(SOURCE_DIR)

    if not files:
        logger.warning("No files found in source directory")
        return

    landing_path = create_landing_path()

    for file in files:
        source_file = os.path.join(SOURCE_DIR, file)

        if not os.path.isfile(source_file):
            continue

        # 🔍 Validation
        if not check_file_exists(source_file):
            logger.error(f"File not found: {source_file}")
            continue

        if not check_file_not_empty(source_file):
            logger.error(f"Empty file: {source_file}")
            continue

        target_file = os.path.join(landing_path, file)

        shutil.move(source_file, target_file)

        logger.info(f"Moved file {file} → {landing_path}")


if __name__ == "__main__":
    logger.info("Starting SFTP Simulation...")
    move_files_to_landing()
    logger.info("SFTP Simulation Completed.")