import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SERVER = 'localhost:9092'
TOPIC = 'cdr-'
CONFIG_FILE = '\\config.yaml'
CELL_TOWER_LOCATIONS = ROOT_DIR+'/docs/TEST_DATA - Cell_Tower_Location.csv'
CUSTOMER_LIST = ROOT_DIR+'/docs/customer_list.csv'