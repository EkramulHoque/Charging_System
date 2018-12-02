import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVER = '142.58.215.106:9092'
TOPIC = 'cdr-'
CONFIG_FILE = '\\config.yaml'
CELL_TOWER_LOCATIONS = ROOT_DIR+'/docs/TEST_DATA - Cell_Tower_Location.csv'
CUSTOMER_LIST = ROOT_DIR+'/docs/customer_list.csv'


#for windows user
#zkserver
# go to your local kafka directory .\bin\windows\kafka-server-start.bat .\config\server.properties
#python org/src/controller/producer.py