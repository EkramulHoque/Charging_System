from kafka import KafkaProducer
import time, threading
import os
import configparser
from org.sfu.billing.simulator.module.datagenerator import generate

def load_properties():
    config_dir = os.environ.get('APP_HOME')
    config_fileName = ('config.ini')
    config_file = config_dir + os.sep + config_fileName
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

# messages per second
# creates kafka instance with server name mentioned in the definitions.py
# creates topic
# sets frequency of data ingestion
# gets the message from simulator
# sends to kafka
def send_at(rate):
    config = load_properties()
    producer = KafkaProducer(bootstrap_servers=[config['KAFKA']['SERVER_IP']+':'+config['KAFKA']['SERVER_PORT']])
    topic = config['KAFKA']['TOPIC_PREFIX'] + str(rate)
    interval = 1 / rate
    while True:
        msg = generate()
        producer.send(topic, msg.encode('ascii'))
        time.sleep(interval)

#uses thread to produce data for each frequency mentioned in the config.yaml
if __name__ == "__main__":
    config = load_properties()
    rates = map(int, config['KAFKA']['RATE'].split(" "))
    #rates = [1,10,100]
    for rate in rates:
        server_thread = threading.Thread(target=send_at, args=(rate,))
        server_thread.setDaemon(True)
        server_thread.start()

    while 1:
        time.sleep(1)
