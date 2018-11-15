import sys,random
from org.src.utility.utils import load_customer,load_yaml
from definitions import ROOT_DIR
assert sys.version_info >= (3, 5)
from datetime import datetime, timedelta

def month_converter(month):
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    return months.index(month) + 1


def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta


def check_event(event):
    event_type = 0
    if event == 'Data':
        event_type = 1
    else:
        event = 0
    return event_type


def generate():
    file_path = ROOT_DIR+"\\config.yaml"
    data = load_yaml(file_path)
    length = data.get('RECORDS')
    event_type = check_event(data.get('EVENT_TYPE'))
    date = data.get('MONTH')
    start = month_converter(date[0])

    if len(date) > 1:
        end = month_converter(date[1])
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
            msg += str(data[j].id) + ", " \
                   + dts[interval]+ ", " \
                   + dts[interval+1] + ", " \
                   + str(data[j].origin) + ", " \
                   + str(data[j].destination) + ", " \
                   + str(data[j].caller)+ ", " \
                   + str(data[j].receiver) + ", " \
                   + str(random.randint(0,1)) + ", " \
                   + str(event_type) + '\n'
    return msg
