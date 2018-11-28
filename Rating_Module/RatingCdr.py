from Rating_Module.data_loader import data_loader
from Rating_Module.Rating import RatingProc


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


    def __aggregate(self):
        print('rating cdr: aggregate')

    def rate(self):
        """
        In this method all rates will be calculated
        :return: return a dataframe of ratings
        """
        self.__aggregate()
        print('RatingCdr')
        return self.cdr_df

    def offer(self):
        """
        This function provide the offers for the customer
        :return: a data frame of offers for all customers
        """
        print("offer")
        self.offer_df = self.cdr_df
        return self.offer_df



