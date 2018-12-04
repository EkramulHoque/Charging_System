from abc import ABC, abstractmethod


class RatingProc(ABC):
    """
    abstract class for rating
    """

    @abstractmethod
    def __aggregate(self):
        pass

    @abstractmethod
    def rate(self):
        print("RatingProc: Rating")
        pass

    @abstractmethod
    def offer(self):
        print("RatingProc: Offer")
        pass

