from datetime import timedelta
# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions, QueryOptions)
import csv
import time
import glob
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
        self.coll_card_name = "vc_card"
        self.coll_log_name = "vc_log"

        self.coll_list = [
            self.coll_import_name,
            self.coll_poi_name,
            self.coll_card_name,
            self.coll_log_name
        ]

        self.cluster = None
        self.bucket = None
        self.scope = None

        self.coll_import = None
        self.coll_poi = None
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
        self.coll_card = self.scope.collection(self.coll_card_name)
        self.coll_log = self.scope.collection(self.coll_log_name)

    def importCSV(self, filename):
        with open(filename, 'r') as csvfile:
            datareader = csv.reader(csvfile)
            for record in datareader:
                date_parts = record[0].split("-")
                date = date_parts[0] + '-' + date_parts[1] + '-20' + date_parts[2]

                date_act_parts = record[5].split("-")
                date_act = date_act_parts[0] + '-' + date_act_parts[1] + '-20' + date_act_parts[2]

                doc = {
                    "date":         date,
                    "time":         record[1],
                    "venue_name":   record[2],
                    "venue_id":     record[3],
                    "card_id":      record[4],
                    "date_activation": date_act,
                    "field1":       record[6],
                    "field2":       record[7],
                    "card_type":    record[8],
                    "filename":     filename
                }
                try:
                    key = doc['date'] + '-' + doc['time'] + '-' + doc['venue_id']
                    self.coll_import.upsert(key, doc)
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
                SUBSTR(date,6,4) year 
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
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)

    def searchVenueLestVisitDay(self, day):
        """
        1- POI che ha avuto il minimo numero di accessi in un giorno specifico.
        """
        qry = """
            SELECT p.*
            FROM verona_card.verona_card_0.vc_poi p
            WHERE 
            date = $1 AND 
            cnt WITHIN (
                SELECT cnt
                FROM verona_card.verona_card_0.vc_poi p2
                WHERE 
                    date = $1
                ORDER BY cnt
                LIMIT 1
            )
        """
        try:
            params = QueryOptions(positional_parameters=[day])
            row_iter = self.cluster.query(qry, params)
            for row in row_iter:
                print(row)
        except Exception as e:
            print(e)

    def searchVenueMostMonthVisitYear(self, year):
        """
        2- dato un anno, trovare per ogni mese il POI con il numero massimo di accessi.
        """
        qry = """
            SELECT p.*
            FROM verona_card.verona_card_0.vc_poi p
            WHERE 
            year = $1 AND (
                ( month = '01' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '01' ORDER BY cnt DESC LIMIT 1 ) ) OR 
                ( month = '02' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '02' ORDER BY cnt DESC LIMIT 1 ) ) OR 
                ( month = '03' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '03' ORDER BY cnt DESC LIMIT 1 ) ) OR 
                ( month = '04' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '04' ORDER BY cnt DESC LIMIT 1 ) ) OR 
                ( month = '05' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '05' ORDER BY cnt DESC LIMIT 1 ) ) OR 
                ( month = '06' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '06' ORDER BY cnt DESC LIMIT 1 ) ) OR 
                ( month = '07' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '07' ORDER BY cnt DESC LIMIT 1 ) ) OR 
                ( month = '08' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '08' ORDER BY cnt DESC LIMIT 1 ) ) OR 
                ( month = '09' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '09' ORDER BY cnt DESC LIMIT 1 ) ) OR 
                ( month = '10' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '10' ORDER BY cnt DESC LIMIT 1 ) ) OR 
                ( month = '11' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '11' ORDER BY cnt DESC LIMIT 1 ) ) OR 
                ( month = '12' AND cnt WITHIN ( SELECT cnt FROM verona_card.verona_card_0.vc_poi p1 WHERE year = $1 AND month = '12' ORDER BY cnt DESC LIMIT 1 ) ) 
            )
            ORDER BY month
        """
        try:
            params = QueryOptions(positional_parameters=[year])
            row_iter = self.cluster.query(qry, params)
            for row in row_iter:
                print(row)
        except Exception as e:
            print(e)

    def searchCardsSwipeMultipleDays(self):
        """
        3- le verona card che hanno fatto strisciate in più di 1 giorno, riportando tutte le strisciate che la carta ha eseguito.
        """
        qry = """
            SELECT c.*
            FROM verona_card.verona_card_0.vc_card c
            WHERE num_days > 1
            ORDER BY cnt DESC
        """
        try:
            row_iter = self.cluster.query(qry)
            for row in row_iter:
                print(row)
        except Exception as e:
            print(e)

    def LogGetData(self, filename):
        qry = """
            SELECT l.*
            FROM verona_card.verona_card_0.vc_log l
            WHERE filename = $1
        """
        params = QueryOptions(positional_parameters=[filename])
        row_iter = self.cluster.query(qry, params)
        results = []
        try:
            for row in row_iter:
                print(row)
                results.append(row)
        except Exception as e:
            print(e)
            results = None
        return results

    def LogAddData(self, filename):
        result = True
        qry = """
            SELECT 
                MIN(STR_TO_MILLIS( i.date ,'DD-MM-YYYY')) timestamp_min, 
                MAX(STR_TO_MILLIS( i.date ,'DD-MM-YYYY')) timestamp_max
            FROM verona_card.verona_card_0.import i
            WHERE filename = $1
        """
        params = QueryOptions(positional_parameters=[filename])
        row_iter = self.cluster.query(qry, params)
        result = None
        try:
            for row in row_iter:
                result = row
                print(result)
                break
        except Exception as e:
            print(e)
            return False

        key = filename.replace(' ', '_')
        doc = {
            'filename':          filename,
            'date_import':       int( time.time() * 1000 ),
            'timestamp_min':     result['timestamp_min'],
            'timestamp_max':     result['timestamp_max']
        }
        try:
            print(result)
            print(doc)
            self.coll_log.upsert(key, doc)
        except Exception as e:
            print(e)
            result = False
        return result

    def InitCollections(self):
        qry_create = "CREATE COLLECTION {}.{}.{} IF NOT EXISTS"
        qry_index = "CREATE PRIMARY INDEX IF NOT EXISTS ON {}.{}.{}"

        try:
            for coll_name in self.coll_list:
                # COLLECTION
                qry = self.cluster.query( qry_create.format(self.bucket_name, self.scope_name, coll_name) )
                qry.execute()

                # INDEX
                qry = self.cluster.query( qry_index.format(self.bucket_name, self.scope_name, coll_name) )
                qry.execute()

        except Exception as e:
            print("ERROR: InitCollections: ")
            print(e)



    def ResetCollections(self):
        qry_delete = "DELETE FROM {}.{}.{}"
        try:
            for coll_name in self.coll_list:
                qry = self.cluster.query(qry_delete.format(self.bucket_name, self.scope_name, coll_name) )
                qry.execute()
                print(qry)
        except Exception as e:
            print("ERROR: ResetCollections: ")
            print(e)



def main():
    cvc = CouchVeronaCard("couchtester", "couchtester", "verona_card", 'verona_card_0')
    cvc.connect()
    cvc.InitCollections()
    cvc.ResetCollections()
    basepath = "./dataset_veronacard_2014_2020/"
    csvfiles = glob.glob(basepath+'*.csv')
    for csvfile in csvfiles:
        print(f"Processing: {csvfile}")
        logs = cvc.LogGetData(csvfile)
        if len(logs)>0:
            print(f"Skippings: {csvfile}")
            continue
        cvc.importCSV(csvfile)
        cvc.LogAddData(csvfile)

        cvc.AggregateByVenueDate()
        cvc.AggregateByCard()

    cvc.searchVenueLestVisitDay('17-08-14')
    cvc.searchVenueMostMonthVisitYear('14')
    cvc.searchCardsSwipeMultipleDays()



if __name__ == '__main__':
    main()
