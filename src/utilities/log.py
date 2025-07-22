import logging
import os
from datetime import datetime

def get_logger(name):
    log_format = "%(asctime)s : %(levelname)s : %(module)s : %(funcName)s() : %(lineno)d : %(message)s"
    
    # Create logs directory if it doesn't exist
    log_dir = "./tmp/logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Generate timestamp for log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"logs_{timestamp}.txt")
    
    # Create handlers for both console and file
    handlers = [
        logging.StreamHandler(),  # Console handler
        logging.FileHandler(log_file)  # File handler
    ]

    logging.basicConfig(level=logging.INFO, format=log_format, handlers=handlers)
    logger = logging.getLogger(name)
    return logger
