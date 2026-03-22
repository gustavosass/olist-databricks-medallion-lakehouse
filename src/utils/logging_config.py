# lakehouse_engine/utils/logging_config.py
import logging

def setup_logging(name):
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s"
    )
    return logging.getLogger(name)