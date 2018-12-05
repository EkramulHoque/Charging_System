from org.sfu.billing.utils.dataLayer import dataLoader
from org.sfu.billing.rating.Rating import Rating
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Rating CDR').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+


class RatingCdr(Rating):
    """
    CDR class for Rating
    """
    def __init__(self):
        """
        This class is a constructor for the
        :param Cdr: the
        """

        dl = dataLoader()
        self.__customers = dl.loadCustomers()
        self.__offers = dl.load_offers()
        self.__medi_df = dl.loadCDR()

    def execute(self, med_df):
        """
        In this method all rates will be calculated
        :param med_df:
        :return: a dataframe of the customers including the rating as well
        """
        med_df =self.__medi_df
        med_df.printSchema()
        """
        med_df = med_df.withColumn('duration', med_df['dateTimeDisconnect']-med_df['dateTimeConnect'])
        med_df = med_df.join(self.__customers, med_df['customerId'] == self.__customers['customerId'])
        # med_df = med_df.drop(med_df['name']).drop(med_df['lastname'])
        med_df = med_df.join(self.__offers, med_df['offerId'] == self.__offers['offerId'])
        med_df = med_df.withColumn('bill', med_df['rate']*med_df['duration'])
        """
        return med_df

    def offer(self):
        """
        This function provide the offers for the customer
        :return: a data frame of offers for all customers
        """
        print("offer")
        self.offer_df = self.cdr_df
        return self.offer_df



if __name__== "__main__":
    rcdr = RatingCdr()
    print(rcdr.show(10))

