from abc import ABC, abstractmethod


class RatingProc(ABC):
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

