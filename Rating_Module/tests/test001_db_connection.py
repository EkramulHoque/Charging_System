from Rating_Module import mlib_connector
import pprint


def test1():
    my_obj = {
        "cname": "ramesh",
        "clast": "katebi"
    }
    cid = mlib_connector.push_record(my_obj)
    c = mlib_connector.customer_by_id(cid)
    for i in c:
        pprint.pprint(i)
    print("hi")


if __name__ == "__main__":
    test1()