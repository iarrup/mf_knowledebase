import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from src.core.config import settings

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "app.log")
LOG_LEVEL = settings.LOG_LEVEL.upper()

# Define the log format
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(LOG_FORMAT)

def setup_logging():
    """Configures the root logger."""
    
    # Ensure log directory exists
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    root_logger = logging.getLogger()
    
    # Check if handlers are already configured
    if root_logger.hasHandlers():
        return

    root_logger.setLevel(LOG_LEVEL)

    # 1. Console Handler (stdout)
    # This is essential for 'docker logs'
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 2. Rotating File Handler
    # This ensures logs are persisted and don't grow indefinitely
    # 10MB per file, 5 backup files
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Set libraries (like httpx) to a higher log level
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    root_logger.info("Logging configured.")