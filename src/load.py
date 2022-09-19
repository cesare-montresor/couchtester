from config import Config
from CVC import CVCLoad


import glob


def load():
    cvc = CVCLoad()

    params = Config.sharedConfig().getConfig('databases', "couchtester_cluster0")
    cvc.connect(**params)

    basepath = "./dataset"
    csvfiles = glob.glob(basepath+'*.csv')
    #csvfiles = ["./dataset/dati_2014.csv"]
    for csvfile in csvfiles:
        print(f"Processing: {csvfile}")
        logs = cvc.logGetData(csvfile)
        if len(logs) > 0:
            print(f"Skippings: {csvfile}")
            continue
        cvc.loadCSV(csvfile)
        cvc.logAddData(csvfile)

    #cvc.createPoiList()
    #cvc.createCalendar(2014)


def main():
    load()

if __name__ == '__main__':
    main()
