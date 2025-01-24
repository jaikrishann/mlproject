

import logging,os
from datetime import datetime
dir_name = "insurance_logs"
os.makedirs(dir_name,exist_ok=True)

time_stamp=datetime.now().strftime("%d-%m-%y %H-%M-%S")
file_name=time_stamp + ".log"

log_file_path = os.path.join(dir_name, file_name)

logging.basicConfig(filename=log_file_path,level=logging.DEBUG,filemode="w",format='%(asctime)s-%(levelname)s-%(message)s')