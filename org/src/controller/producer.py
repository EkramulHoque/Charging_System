from kafka import KafkaProducer
import time, threading
from org.src.module.loader import generate
from org.src.utility.utils import load_yaml
from definitions import ROOT_DIR

file_path = ROOT_DIR+"\\config.yaml"
data = load_yaml(file_path)
rates = data.get('Rate')# messages per second


def send_at(rate):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    topic = 'cdr-' + str(rate)
    interval = 1 / rate
    while True:
        msg = generate()
        producer.send(topic, msg.encode('ascii'))
        time.sleep(interval)


if __name__ == "__main__":
    for rate in rates:
        server_thread = threading.Thread(target=send_at, args=(rate,))
        server_thread.setDaemon(True)
        server_thread.start()

    while 1:
        time.sleep(1)
