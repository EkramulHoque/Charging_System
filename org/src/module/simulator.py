import sys,random
from random import shuffle,seed
from org.src.utility.data_loader import load_customer,load_yaml
from definitions import ROOT_DIR, CONFIG_FILE
assert sys.version_info >= (3, 5)
from datetime import datetime, timedelta


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

#checks the type of event mentioned in config yaml
def check_event(event):
    event_type = 0
    if event == 'Data':
        event_type = 1
    else:
        event_type = 0
    return event_type

#1. load configurations from config.yaml
#2. Get the size of records i.e "length",event type ,month(mapped to index)
#3. Create a list of datetime values
#4. load customer list created from in data_loader
#5. Create the message adding the call status and event type as the last two parameters
#6. example output : TESTCUST12,20181101 154530,20181101 164530,49.252121 | -122.893949,49.252814 | -122.896873,2365482589,2365694587,0,1
def generate():
    file_path = ROOT_DIR+CONFIG_FILE
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
    msg = dict()
    msg1 = dict()
    end_date_time = []
    call_status = dict()
    final = ""
    for i in range(1,length):
        for j in data:
            start_date_time = random.randint(0,len(dts)-2)
            end_date_time.append(start_date_time + 1)
            call_status_temp = random.randint(0,1)
            call_status[j] = call_status_temp
            msg[j] =str(j+1) + ", " \
                   + str(data[j].id) + ", " \
                   + dts[start_date_time]+ ", " \
                   + str(None) + ", " \
                   + str(data[j].origin) + ", " \
                   + str(data[j].destination) + ", " \
                   + str(data[j].caller)+ ", " \
                   + str(data[j].receiver) + ", " \
                   + str(call_status_temp) + ", " \
                   + str(event_type) + '\n'

        for k in data:
            msg1[k] = str(k+1) + ", " \
                     +str(data[k].id) + ", " \
                     + str(None) + ", " \
                     + dts[end_date_time[k]] + ", " \
                     + str(data[k].origin) + ", " \
                     + str(data[k].destination) + ", " \
                     + str(data[k].caller) + ", " \
                     + str(data[k].receiver) + ", " \
                     + str(call_status[k]) + ", " \
                     + str(event_type) + '\n'
        shuffle(msg)
        shuffle(msg1)
        for l in msg:
            final += msg1[l] + msg[l]
    return final

# if __name__ == "__main__":
#     result = generate()
#     print(result)