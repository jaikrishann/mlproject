from source_code.logger import logging
from source_code.execption import InsuranceException
import sys 

# logging.info("hello world")
# logging.debug("hello world")
# logging.warning("hello world")
# logging.critical("hello world")
# logging.error("hello world")


try:
    10/0
except Exception as e:

    obj =  InsuranceException(error_message=e,error_details= sys )
    logging.warning(obj.error_message)
    print(obj.error_message)

