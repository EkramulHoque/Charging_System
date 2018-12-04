import sys
from kafka import KafkaConsumer
from definitions import ROOT_DIR, SERVER, TOPIC, CONFIG_FILE
topic = sys.argv[1]
consumer = KafkaConsumer(topic, bootstrap_servers=[SERVER])
for msg in consumer:
    print(msg.value.decode('utf-8'))