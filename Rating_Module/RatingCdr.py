from Rating_Module.data_loader import data_loader
from Rating_Module.Rating import RatingProc
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Rating CDR').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+


class RatingCdr(RatingProc):
    """
    CDR class for Rating
    """
    def __init__(self, Cdr):
        """
        This class is a constructor for the
        :param Cdr: the
        """
        self.__cdr_df = Cdr
        dl = data_loader()
        self.__customers = dl.load_customer()
        self.__offers = dl.load_offers()

    def execute(self, med_df):
        """
        In this method all rates will be calculated
        :return: return a dataframe of ratings
        """
        med_df = med_df.withColumn('duration', med_df['dateTimeDisconnect']-med_df['dateTimeConnect'])
        med_df = med_df.join(self.__customers, med_df['customerId'] == self.__customers['customerId'])
        # med_df = med_df.drop(med_df['name']).drop(med_df['lastname'])
        med_df = med_df.join(self.__offers, med_df['offerId'] == self.__offers['offerId'])
        med_df = med_df.withColumn('bill', med_df['rate']*med_df['duration'])
        return med_df

    def offer(self):
        """
        This function provide the offers for the customer
        :return: a data frame of offers for all customers
        """
        print("offer")
        self.offer_df = self.cdr_df
        return self.offer_df



