from config import Config
from CVC import *
from CVC import CVCLoad

import glob

def aggregate():


    cvcload = CVCLoad()
    cvc = CVCAggregate()

    params = Config.sharedConfig().getConfig('databases', "couchtester_cluster0")

    cvcload.connect(**params)

    cvc.connect(**params)

    loglist = cvcload.logGetData()
    filenames = [entry["filename"] for entry in loglist]
    #filenames = ["./dataset/dati_2014.csv"]
    for filename in filenames:
        print("AggregateDataFrom:" + filename)
        cvc.aggregateByVenueDate(filename)
        cvc.aggregateByCard(filename)

def main():
    aggregate()

if __name__ == '__main__':
    main()
