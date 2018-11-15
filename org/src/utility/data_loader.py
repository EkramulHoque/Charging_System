import csv, random, string
import yaml, sys
from definitions import CELL_TOWER_LOCATIONS, CUSTOMER_LIST
assert sys.version_info >= (3, 5)


class Customer:
    'Common base class for all customer'
    customer_count = 0

    def __init__(self, id, caller, receiver, origin, destination):
        self.id = id
        self.caller = caller
        self.receiver = receiver
        self.origin = origin
        self.destination = destination
        Customer.customer_count += 1

    def displayCount(self):
        print("Total Customer" + str(Customer.customer_count))

    def displayCustomer(self):
        print("ID : " + str(self.id) + " Phone: " + str(self.caller))

def load_locations():
    locations_list = dict()
    with open(CELL_TOWER_LOCATIONS, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            locations_list[line_count] = row['nodeId']
            line_count += 1
    return locations_list,line_count


def load_customer():
    customer_list = dict()
    locations,location_count = load_locations()
    with open(CUSTOMER_LIST, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        line_count = 0
        for row in csv_reader:
            sindex = random.randint(0,location_count-1)
            source = locations[sindex]
            dindex = random.randint(0,location_count-1)
            while sindex == dindex:
                dindex = random.randint(0,location_count-1)
            destination = locations[dindex]
            alpa_id = ''.join(random.choice(string.ascii_uppercase) for _ in range(8))
            customer_list[line_count] = Customer(alpa_id + str("%02d" % (line_count,)), row["caller"], row["reciever"],
                                                 source,destination)
            line_count += 1
    return customer_list


def load_yaml(filepath):
    "Loads a yaml file"
    with open(filepath, "r") as file_descriptor:
        data = yaml.load(file_descriptor)
    return data


def dump_yaml(filepath, data):
    "Dumps data"
    with open(filepath, "w") as file_descriptor:
        yaml.dump(data, file_descriptor)
