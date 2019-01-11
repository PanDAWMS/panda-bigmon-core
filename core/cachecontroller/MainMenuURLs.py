import AbstractTasksProvider


class MainMenuURLs(AbstractTasksProvider):

    BASIC_PRIORITY = 1

    def getvalidityperiod(self):
        return 20

    def getaggressiveness(self):
        return 2

    def getpayload(self):
        urlsList = {}
        with open('mainmenurls.txt') as urls:
            for line in urls:
                line = line.rstrip('\r\n')
                urlsList[line] = self.BASIC_PRIORITY
        return urlsList

