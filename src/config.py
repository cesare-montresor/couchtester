#import yaml

class Config:
    __sharedConfig = None

    @staticmethod
    def sharedConfig():
        if Config.__sharedConfig is None:
            __sharedConfig = Config()
        return __sharedConfig


    def __init__(self):
        self.config = {
            "databases": {
                "couchtester_cluster0": {
                    "url": "couchbase://172.31.1.1",
                    "username": "couchtester",
                    "password": "couchtester",
                    "bucket_name": "verona_card",
                    "scope_name": 'verona_card_0'
                },
            }
        }

    #def loadConfig(self, path):
    #    with open(path) as f:
    #        self.config = yaml.load(f, Loader=yaml.FullLoader)

    def getConfig(self,section,name):
        return self.config[section][name];
