import logging

def show_error_message(error_string_prefix,error_string_suffix ):
    error_message = error_string_prefix + " = " + error_string_suffix
    print(error_message)

def setup_logging(log_filename='app.log'):
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger('ETLLogger')
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(logging.Formatter(log_format))
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger