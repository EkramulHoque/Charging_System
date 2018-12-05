from abc import ABC, abstractmethod


class Rating(ABC):
    """
    abstract class for rating
    """

    @abstractmethod
    def execute(self):
        print("RatingProc: Rating")
        pass

    @abstractmethod
    def offer(self):
        print("RatingProc: Offer")
        pass

