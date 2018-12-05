import random
import configparser
import os
from org.sfu.billing.simulator.utility.data_loader import load_customer
from datetime import datetime, timedelta


def load_properties():
    config_dir = os.environ.get('APP_HOME')
    config_fileName = ('config.ini')
    config_file = config_dir + os.sep + config_fileName
    return config_file,config_dir

#maps month to index (string -> int)
def month_converter(month):
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    return months.index(month) + 1

#creates datetime value for a given time range (start,end) and an interval
def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

#1. load configurations from config.ini
#2. Get the size of records i.e "length",event type ,month(mapped to index)
#3. Create a list of datetime values
#4. load customer list created from in data_loader
#5. Create the message adding the call status and event type as the last two parameters
#6. example output : TESTCUST12,20181101 154530,20181101 164530,49.252121 | -122.893949,49.252814 | -122.896873,2365482589,2365694587,0,1
def generate():
    config = configparser.ConfigParser()
    config_file, config_dir = load_properties()
    config.read(config_file)
    length = int(config['KAFKA']['RECORDS'])
    event_type = config['KAFKA']['EVENT']
    date = config['KAFKA']['MONTH']
    start = month_converter(date)

    if len(date) > 1:
        end = month_converter(date)
    else:
        end = start

    dts = [dt.strftime('%Y%m%d %H%M%S') for dt in
           datetime_range(datetime(2018, start, 1, random.randint(0, 23)), datetime(2018, end, 31, random.randint(0, 23)),
                          timedelta(minutes=random.randint(1, 60)))]
    data = load_customer()
    msg = ''
    for i in range(1,length):
        for j in data:
            interval = random.randint(0,len(dts)-2)
            # msg += str(data[j].id) + ", " \
            #        + str(dts[interval])+ ", " \
            #        + str(dts[interval+1]) + ", " \
            #        + str(data[j].origin) + ", " \
            #        + str(data[j].destination) + ", " \
            #        + str(data[j].caller)+ ", " \
            #        + str(data[j].receiver) + ", " \
            #        + str(random.randint(0,1)) + ", " \
            #        + str(event_type) + '\n'
            msg += str(dts[interval]) + ", " \
                   + str(dts[interval + 1]) + ", " \
                   + str(data[j].origin) + ", " \
                   + str(data[j].destination) + ", " \
                   + str(data[j].caller) + ", " \
                   + str(data[j].receiver) + ", " \
                   + str(random.randint(0, 1)) + ", " \
                   + str(event_type) + '\n'
    return msg

# if __name__ == "__main__":
#     print(generate())