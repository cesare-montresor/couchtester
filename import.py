from datetime import timedelta
from model import ModelCardSwipe
# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions, QueryOptions)
import csv


"""
1- istanza di info specifica: POI che ha avuto il minimo numero di accessi in un giorno specifico 
2- dato un anno, trovare per ogni mese il POI con il numero massimo di accessi
3- le verona card che hanno fatto strisciate in più di 1 giorno, riportando tutte le strisciate che la carta ha eseguito.


una soluzione ottimizzata per l'inserimento
una soluzione ottimizzata per le interrogazioni


DATASET: VeronaCard




Si tratta delle strisciate delle VeronaCard dal 2014 al 2020 (un file CSV per ogni anno). La struttura di tutti i file è la seguente:
    Data ingresso
    Ora ingresso 
    POI
    Dispositivo: codice identificativo del dispositivo su cui è stata fatta la strisciata (potrebbero essercene più d'uno per lo stesso POI)
    Seriale VeronaCard
    Data Attivazione
    Profilo: tipo di VC (24-ore, 48-ore, ecc...)
"""


class CouchVeronaCard():
    def __init__(self,username, password, bucket_name, scope_name, cert_path=None):
        self.username = username
        self.password = password
        self.cert_path = cert_path

        self.bucket_name = bucket_name
        self.scope_name = scope_name

        self.coll_import_name = "import"
        self.coll_poi_name = "vc_poi"
        self.coll_time_name = "vc_time"
        self.coll_card_name = "vc_card"
        self.coll_log_name = "vc_log"

        self.cluster = None
        self.bucket = None
        self.scope = None

        self.coll_import = None
        self.coll_poi = None
        self.coll_time = None
        self.coll_card = None
        self.coll_log = None


    def connect(self):
        # Update this to your cluster

        # User Input ends here.

        # Connect options - authentication
        auth = PasswordAuthenticator(self.username,self.password)

        # Get a reference to our cluster
        # NOTE: For TLS/SSL connection use 'couchbases://<your-ip-address>' instead
        self.cluster = Cluster('couchbase://localhost', ClusterOptions(auth))

        # Wait until the cluster is ready for use.
        self.cluster.wait_until_ready(timedelta(seconds=5))

        # get a reference to our bucket
        self.bucket = self.cluster.bucket(self.bucket_name)
        self.scope = self.bucket.scope(self.scope_name)

        self.coll_import = self.scope.collection(self.coll_import_name)
        self.coll_poi = self.scope.collection(self.coll_poi_name)
        self.coll_time = self.scope.collection(self.coll_time_name)
        self.coll_card = self.scope.collection(self.coll_card_name)
        self.coll_log = self.scope.collection(self.coll_log_name)

    def importCSV(self, filename):
        with open(filename, 'r') as csvfile:
            datareader = csv.reader(csvfile)
            for record in datareader:
                doc = ModelCardSwipe(*record)
                try:
                    key = doc.date + '-' + doc.time + '-' + doc.venue_id
                    self.coll_import.upsert(key, doc.toDictionary())
                except Exception as e:
                    print(e)

    def AggregateByVenueDate(self):
        """
        1- POI che ha avuto il minimo numero di accessi in un giorno specifico.
        2- dato un anno, trovare per ogni mese il POI con il numero massimo di accessi.
        """
        qry = """
            SELECT 
                count(*) cnt, 
                venue_name, 
                date, 
                ARRAY_AGG( {time, card_id, card_type} ) AS swipe,
                SUBSTR(date,0,2) day,
                SUBSTR(date,3,2) month,
                SUBSTR(date,6,2) year 
            FROM verona_card.verona_card_0.import i 
            GROUP BY venue_name, date
        """

        try:
            row_iter = self.cluster.query(qry)
            cntr = 0
            for row in row_iter:
                try:
                    key = row['venue_name'].replace(' ', '_') + '_' + row['date']
                    self.coll_poi.upsert(key, row)
                    cntr += 1
                except Exception as e:
                    print(f"AggregateByVenueDate:write:\n {e}")
        except Exception as e:
            print(f"AggregateByVenueDate:read:\n {e}")

    def AggregateByCard(self):
        """
        le verona card che hanno fatto strisciate in più di 1 giorno, riportando tutte le strisciate che la carta ha eseguito.
        """
        qry = """
            SELECT 
                count(*) cnt, 
                card_id, 
                ARRAY_AGG( {date, time, venue_name, venue_id } ) AS swipe 
            FROM verona_card.verona_card_0.import i 
            GROUP BY card_id
        """
        try:
            row_iter = self.cluster.query( qry )
            cntr = 0
            for row in row_iter:
                try:
                    key = row['card_id']
                    days = [swipe['date'] for swipe in row['swipe']]
                    row['num_days'] = len(set(days))
                    self.coll_card.upsert(key, row)
                    cntr += 1
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)
        print(cntr)

    def searchByCard(self):
        """
        1- POI che ha avuto il minimo numero di accessi in un giorno specifico.
        2- dato un anno, trovare per ogni mese il POI con il numero massimo di accessi.
        3- le verona card che hanno fatto strisciate in più di 1 giorno, riportando tutte le strisciate che la carta ha eseguito.
        """

def main():
    cvc = CouchVeronaCard("Administrator", "123456", "verona_card", 'verona_card_0')
    cvc.connect()
    csvpath = "./dataset_veronacard_2014_2020/dati_2014.csv"
    # cvc.importCSV(csvpath)
    #cvc.AggregateByVenueDate()
    cvc.AggregateByCard()





"""
# upsert document function
def upsert_document(doc):
    print("\nUpsert CAS: ")
    try:
        # key will equal: "airline_8091"
        key = doc["type"] + "_" + str(doc["id"])
        result = cb_coll.upsert(key, doc)
        print(result.cas)
    except Exception as e:
        print(e)

# get document function


def get_airline_by_key(key):
    print("\nGet Result: ")
    try:
        result = cb_coll.get(key)
        print(result.content_as[str])
    except Exception as e:
        print(e)

# query for new document by callsign


def lookup_by_callsign(cs):
    print("\nLookup Result: ")
    try:
        sql_query = 'SELECT VALUE name FROM `travel-sample`.inventory.airline WHERE callsign = $1'
        row_iter = cluster.query(
            sql_query,
            QueryOptions(positional_parameters=[cs]))
        for row in row_iter:
            print(row)
    except Exception as e:
        print(e)


airline = {
    "type": "airline",
    "id": 8091,
    "callsign": "CBS",
    "iata": None,
    "icao": None,
    "name": "Couchbase Airways",
}

upsert_document(airline)

get_airline_by_key("airline_8091")

lookup_by_callsign("CBS")
"""

if __name__ == '__main__':
    main()
