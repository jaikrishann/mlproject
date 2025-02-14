import sys 
class InsuranceException(Exception):
    def __init__(self,error_message : Exception,error_details : sys):
        super().__init__(error_message)
        self.error_message = InsuranceException.error_message_detail(error_message,error_details=error_details)


    @staticmethod
    def error_message_detail(error:Exception,error_details : sys)->str:
        error_class,error_message,exc_tb = error_details.exc_info()
        line_number = exc_tb.tb_frame.f_lineno
        file_name = exc_tb.tb_frame.f_code.co_filename

        error_message = f"error occured in this{file_name}file,at the line no. [{line_number}], error message:- [{error_message}]"
        return error_message
    

    def __str__(self):
        return InsuranceException.__name__.__str__()