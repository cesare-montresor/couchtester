from config import Config
from CVC import CVCSchema


def schema(clean=False):
    cvc = CVCSchema()

    params = Config.sharedConfig().getConfig('databases', "couchtester_cluster0")
    cvc.connect(**params)

    if clean:
        cvc.DropCollections()

    cvc.CreateCollections()
    cvc.IndexCollections()


def main():
    # schema(clean=True)
    schema(clean=False)

if __name__ == '__main__':
    main()
