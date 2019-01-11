from abc import ABCMeta, abstractmethod

class AbstractTasksProvider:
    __metaclass__ = ABCMeta

    @abstractmethod
    def getpayload(self):
        pass

    @abstractmethod
    def getvalidityperiod(self):
        pass

    @abstractmethod
    def getaggressiveness(self):
        pass

    @abstractmethod
    def getaggressiveness(self):
        # tasks executed:
        # 0 - one-by-one basing on the articular queue priority
        # 1 - in parallel with basic competition
        # 2 - all tasks should be completed within the validity time interval in exception to 0 priority
        pass
