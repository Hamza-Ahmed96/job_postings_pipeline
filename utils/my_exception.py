import sys
from loguru import logger

def error_message_details(error, error_details: sys):
    
    _,_,exc_tb = error_details.exc_info()
    
    file_name = exc_tb.tb_frame.f_code.co_filename
    line_number = exc_tb.tb_lineno
    message = str(error)
    
    return f"Error {message} occured in file : {file_name} at line : {line_number}"


class CustomException(Exception):
    def __init__(self, error_message, error_details : sys):
        super().__init__(error_message)
        self.error_message = error_message_details(error=error_message, error_details=error_details)
        logger.error(self.error_message)
        
    def __str__(self):
        return self.error_message