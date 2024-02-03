import logging
import json
import os
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
def setup_logger(log_file='logs/pipeline.log'):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            log_record = {
                'timestamp': self.formatTime(record, self.datefmt),
                'level': record.levelname,
                'message': record.getMessage(),
                'module': record.module,
                'line': record.lineno,
                'pathname': record.pathname
            }
            return json.dumps(log_record)

    json_formatter = JsonFormatter()
    file_handler.setFormatter(json_formatter)

    logger = logging.getLogger('')
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

    return logger
