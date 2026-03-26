from utils.my_exception import CustomException
import sys

def handle_exceptions(function):
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception as e:
            raise CustomException(e, sys)
    return wrapper
