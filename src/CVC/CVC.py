from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster

from datetime import timedelta

class CVC():

    def __init__(self):
        self.url = None
        self.username = None
        self.password = None
        self.cert_path = None

        self.bucket_name = None
        self.scope_name = None

        self.coll_import_name = "import"
        self.coll_poi_name = "vc_poi"
        self.coll_card_name = "vc_card"
        self.coll_log_name = "vc_log"

        #self.coll_poi_list_name = "vc_poi_list"
        #self.coll_calendar_name = "vc_calendar"

        self.coll_list = [
            self.coll_import_name,
            self.coll_poi_name,
            self.coll_card_name,
            self.coll_log_name,
            #self.coll_poi_list_name,
            #self.coll_calendar_name
        ]

        self.cluster = None
        self.bucket = None
        self.scope = None

        self.coll_import = None
        self.coll_poi = None
        self.coll_card = None
        self.coll_log = None
        self.coll_poi_list = None
        self.coll_calendar = None


    def connect(self, url ,username, password, bucket_name, scope_name, cert_path=None):
        self.url = url
        self.username = username
        self.password = password
        self.cert_path = cert_path

        self.bucket_name = bucket_name
        self.scope_name = scope_name
        # Update this to your cluster

        # User Input ends here.

        # Connect options - authentication
        auth = PasswordAuthenticator(self.username, self.password)
        # Get a reference to our cluster
        # NOTE: For TLS/SSL connection use 'couchbases://<your-ip-address>' instead
        #
        # Increase index timeout
        # curl -X POST -u couchtester:couchtester http://localhost:9102/settings --data '{"indexer.settings.scan_timeout": 300000}'
        # curl -X POST -u couchtester:couchtester http://172.31.1.1:9102/settings --data '{"indexer.settings.scan_timeout": 300000}'
        #
        # Increase other timeouts
        # cluster_timeout = ClusterTimeoutOptions(
        #     bootstrap_timeout =timedelta(minutes=30),
        #     resolve_timeout =timedelta(minutes=30),
        #     connect_timeout =timedelta(minutes=30),
        #     kv_timeout =timedelta(minutes=30),
        #     kv_durable_timeout =timedelta(minutes=30),
        #     views_timeout =timedelta(minutes=30),
        #     query_timeout =timedelta(minutes=30),
        #     analytics_timeout =timedelta(minutes=30),
        #     search_timeout =timedelta(minutes=30),
        #     management_timeout =timedelta(minutes=30),
        #     dns_srv_timeout =timedelta(minutes=30)
        # )
        # self.cluster = Cluster('couchbase://localhost', ClusterOptions(auth, timeout_options = cluster_timeout))
        self.cluster = Cluster(self.url, ClusterOptions(auth))

        # Wait until the cluster is ready for use.
        self.cluster.wait_until_ready(timedelta(seconds=5))

        # get a reference to our bucket
        self.bucket = self.cluster.bucket(self.bucket_name)
        self.scope = self.bucket.scope(self.scope_name)

        self.coll_import = self.scope.collection(self.coll_import_name)
        self.coll_poi = self.scope.collection(self.coll_poi_name)
        self.coll_card = self.scope.collection(self.coll_card_name)
        self.coll_log = self.scope.collection(self.coll_log_name)
        #self.coll_poi_list = self.scope.collection(self.coll_poi_list_name)
        #self.coll_calendar = self.scope.collection(self.coll_calendar_name)

    def printError(self,e):
        """
        if isinstance(e, str):
            print(e)
            return

        context = QueryErrorContext
        """
        print(e)
        return
