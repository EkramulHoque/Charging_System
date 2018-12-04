import sys
import spark
"""
This is class is used to provide the required functions to load and save data to and from database.
I implement  the singleton structure in this class.

"""

class dataLoader2:

    # Here will be the instance stored.
    __instance = False
    __customers = None
    __offers = None

    @staticmethod
    def loadCustomers():
        """ Static access method. """
        try:
            if not dataLoader2.__instance:
                dataLoader2()
            return dataLoader2.__customers
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise
    @staticmethod
    def loadOffers():
        """ Static access method. """
        try:
            if not dataLoader2.__instance:
                dataLoader2()
            return dataLoader2.__offers
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

    def __init__(self):
        """ Virtually private constructor. """
        if not dataLoader2.__instance:
            try:
                dataLoader2.__customers = "customers"
                dataLoader2.__offers = 'offers'
                dataLoader2.__instance = True
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise
        else:
            raise Exception("This class is a singleton!")


