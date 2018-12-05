import sys
from org.sfu.billing.utils.configurations import SparkConfig

"""
This is class is used to provide the required functions to load and save data to and from database.
I implement  the singleton structure in this class.

"""
def singleton(class_):
    class class_w(class_):
        _instance = None
        def __new__(class_, *args, **kwargs):
            if class_w._instance is None:
                class_w._instance = super(class_w, class_).__new__(class_,*args,**kwargs)
                class_w._instance._sealed = False
            return class_w._instance
        def __init__(self, *args, **kwargs):
            if self._sealed:
                return
            super(class_w, self).__init__(*args, **kwargs)
            self._sealed = True
    class_w.__name__ = class_.__name__
    return class_w

@singleton
class dataLoader:

    # Here will be the instance stored.
    __instance = None
    __customers = None
    __offers = None

    def loadCustomers(self):
        """ Static access method. """
        try:
            print(self.__customers.show(10))
            return self.__customers
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise


    def loadOffers(self):
        """ Static access method. """
        try:
            print(self.__offers.show(10))
            return self.__offers
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise


    def __init__(self):
        """ Virtually private constructor. """
        if  self.__instance==None:
            try:
                spark = SparkConfig.get_spark()
                self.__customers = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
                    .option("collection", "customers")\
                    .load()
                self.__customers.printSchema()
                self.__offers = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                    .option("collection","offers").load()
                self.__instance = True
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise Exception("This is a Spark reading from MongoDb Exception ")
        else:
            raise Exception("This class is a singleton Exception")


if __name__== "__main__":
    dl=dataLoader()
    customers = dl.loadCustomers()
    print(customers.show(10))
