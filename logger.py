# logger.py

import logging
import requests
import json
import time

def normalize_log_level(raw_level):
    if isinstance(raw_level, int):
        return raw_level
    
    if isinstance(raw_level, str):
        s = raw_level.strip()

        if s.isdigit():
            return int(s)

        level = s.upper()
        if level == "CRITICAL":
            return logging.CRITICAL
        elif level == "ERROR":
            return logging.ERROR
        elif level == "WARNING":
            return logging.WARNING
        elif level == "INFO":
            return logging.INFO
        elif level == "DEBUG":
            return logging.DEBUG
        else:
            return logging.INFO
            
    return logging.INFO

class LokiHandler(logging.Handler):
    def __init__(self, url, tags=None, level=logging.NOTSET):
        super().__init__(level)
        self.url = url
        self.tags = tags or {}

    def emit(self, record):
        try:
            log_entry = self.format(record)
            ts = str(int(time.time() * 1e9))  # nanoseconds timestamp
            payload = {
                "streams": [
                    {
                        "stream": self.tags,
                        "values": [[ts, log_entry]],
                    }
                ]
            }
            response = requests.post(
                self.url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=5,
            )
            response.raise_for_status()
        except Exception as e:
            print(f"[LokiHandler] Failed to send log to Loki: {e}")

def setup_logger(app_name="DSxOS_python_application", log_file="query.log", loki_url=None, loki_tags=None, level=logging.INFO):
    log_level = normalize_log_level(level)
    
    logger = logging.getLogger(app_name)
    logger.setLevel(log_level)
    logger.handlers = []  # Clear existing handlers if rerun

    # File Handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(log_level)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)

    # Console Handler
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(ch)

    # Loki Handler (optional)
    if loki_url:
        loki_handler = LokiHandler(url=loki_url, tags=loki_tags)
        loki_handler.setLevel(log_level)
        loki_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(loki_handler)

    return logger
