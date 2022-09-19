from config import Config
from CVC import CVCQuery

def query():
    cvc = CVCQuery()

    params = Config.sharedConfig().getConfig('databases', "couchtester_cluster0")
    cvc.connect(**params)

    cvc.searchVenueLestVisitDay('17-08-2014')
    cvc.searchVenueMostMonthVisitYear('2014')
    cvc.searchCardsSwipeMultipleDays()


def main():
    query()

if __name__ == '__main__':
    main()
