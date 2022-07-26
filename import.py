from datetime import timedelta, date, datetime
# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions, QueryOptions)
import csv
import time
import glob
import pandas as pd
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


    def connect(self):
        # Update this to your cluster

        # User Input ends here.

        # Connect options - authentication
        auth = PasswordAuthenticator(self.username,self.password)
        # Get a reference to our cluster
        # NOTE: For TLS/SSL connection use 'couchbases://<your-ip-address>' instead
        #
        # Increase index timeout
        # curl -X POST -u couchtester:couchtester http://localhost:9102/settings --data '{"indexer.settings.scan_timeout": 300000}'
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
                date_event_parts = record[0].split("-")
                date_event = date_event_parts[0] + '-' + date_event_parts[1] + '-20' + date_event_parts[2]

                date_act_parts = record[5].split("-")
                date_act = date_act_parts[0] + '-' + date_act_parts[1] + '-20' + date_act_parts[2]

                timestamp_event = datetime.strptime(date_event, "%d-%m-%Y").timestamp()

                doc = {
                    "date_event":   date_event,
                    "timestamp_event": timestamp_event * 1000,
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
                    key = doc['date_event'] + '-' + doc['time'] + '-' + doc['venue_id']
                    self.coll_import.upsert(key, doc)
                except Exception as e:
                    print(e)

    def aggregateByVenueDate(self, filename):
        """
        1- POI che ha avuto il minimo numero di accessi in un giorno specifico.
        2- dato un anno, trovare per ogni mese il POI con il numero massimo di accessi.
        """
        qry = """
            SELECT 
                count(*) cnt, 
                i.venue_name, 
                i.date_event, 
                ARRAY_AGG( {i.time, i.card_id, i.card_type} ) AS swipe,
                SUBSTR(i.date_event,0,2) day,
                SUBSTR(i.date_event,3,2) month,
                SUBSTR(i.date_event,6,4) year 
            FROM 
                verona_card.verona_card_0.import i RIGHT JOIN 
                (SELECT DISTINCT venue_name FROM verona_card.verona_card_0.import ) AS p ON i.venue_name = p.venue_name
            WHERE 
                i.filename = $1 
            GROUP BY i.venue_name, i.date_event
        """

        try:
            params = QueryOptions(positional_parameters=[filename], timeout=timedelta(minutes=10))
            row_iter = self.cluster.query(qry, params)
            cntr = 0
            for row in row_iter:
                try:
                    key = row['venue_name'].replace(' ', '_') + '_' + row['date_event']
                    self.coll_poi.upsert(key, row)
                    cntr += 1
                except Exception as e:
                    print(f"AggregateByVenueDate:write:\n {e}")
        except Exception as e:
            print(f"AggregateByVenueDate:read:\n {e}")

    def aggregateByCard(self, filename):
        """
        le verona card che hanno fatto strisciate in più di 1 giorno, riportando tutte le strisciate che la carta ha eseguito.
        """
        qry = """
            SELECT 
                count(*) cnt, 
                card_id, 
                ARRAY_AGG( {date_event, time, venue_name, venue_id } ) AS swipe 
            FROM verona_card.verona_card_0.import i 
            WHERE filename = $1
            GROUP BY card_id
        """
        try:
            params = QueryOptions(positional_parameters=[filename])
            row_iter = self.cluster.query( qry, params )
            cntr = 0
            for row in row_iter:
                try:
                    key = row['card_id']
                    days = [swipe['date_event'] for swipe in row['swipe']]
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
            date_event = $1 AND 
            cnt WITHIN (
                SELECT cnt
                FROM verona_card.verona_card_0.vc_poi p2
                WHERE 
                    date_event = $1
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

    def logGetData(self, filename):
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

    def logAddData(self, filename):
        result = True
        qry = """
            SELECT 
                MIN(STR_TO_MILLIS( i.date_event ,'DD-MM-YYYY')) timestamp_min, 
                MAX(STR_TO_MILLIS( i.date_event ,'DD-MM-YYYY')) timestamp_max
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


    def DropCollections(self):
        print("DropCollections ")
        qry_delete = "DROP COLLECTION {}.{}.{} IF EXISTS"
        try:
            for coll_name in self.coll_list:
                print("DropCollections: "+coll_name)
                opt = QueryOptions(timeout=timedelta(minutes=10))
                qry = self.cluster.query(qry_delete.format(self.bucket_name, self.scope_name, coll_name), opt)
                qry.execute()
        except Exception as e:
            print("ERROR: DropCollections: ")
            print(e)

    def CreateCollections(self):
        qry_create = "CREATE COLLECTION {}.{}.{} IF NOT EXISTS"

        try:
            for coll_name in self.coll_list:
                print("CreateCollections: " + coll_name)
                # COLLECTION
                qry = self.cluster.query( qry_create.format(self.bucket_name, self.scope_name, coll_name) )
                qry.execute()
        except Exception as e:
            print("ERROR: CreateCollections: ")
            print(e)

    def IndexCollections(self):
        qry_index = "CREATE PRIMARY INDEX IF NOT EXISTS ON {}.{}.{}"

        custom_idx = [
            "create index idx_import_venue_name ON verona_card.verona_card_0.import(venue_name)",
            "create index idx_import_filename ON verona_card.verona_card_0.import(filename)",
            "create index idx_import_card_id ON verona_card.verona_card_0.import(card_id)",

            "create index idx_vc_poi_month ON verona_card.verona_card_0.vc_poi(month)",
            "create index idx_vc_poi_year ON verona_card.verona_card_0.vc_poi(year)",
            "create index idx_vc_poi_cnt ON verona_card.verona_card_0.vc_poi(cnt)",

            "create index idx_vc_card_num_days ON verona_card.verona_card_0.vc_card(num_days)",
            "create index idx_vc_card_cnt ON verona_card.verona_card_0.vc_card(cnt)"
        ]


        try:
            for coll_name in self.coll_list:
                print("IndexCollections: " + coll_name)
                # INDEX
                qry = self.cluster.query(qry_index.format(self.bucket_name, self.scope_name, coll_name))
                qry.execute()

            for qry_idx in custom_idx:
                print("IndexCollections: custom:" + qry_idx)
                # INDEX
                qry = self.cluster.query( qry_idx )
                qry.execute()

        except Exception as e:
            print("ERROR: IndexCollections: ")
            print(e)




    def createPoiList(self):
        qry = "select distinct i.venue_name from verona_card.verona_card_0.import i"
        try:
            row_iter = self.cluster.query(qry)
            for row in row_iter:
                try:
                    key = row['venue_name']
                    self.coll_poi_list.upsert(key, row)
                except Exception as e:
                    print(e)
        except Exception as e:
            print(e)

    def createCalendar(self, year_from, year_to=None):
        if year_to is None: year_to = date.today().year

        start_day = f'1/1/{year_from}'
        end_day = f'31/12/{year_to}'

        start_date = pd.to_datetime(start_day, format="%d/%m/%Y")
        end_date = pd.to_datetime(end_day, format="%d/%m/%Y")

        date_index = pd.date_range(start_date, end_date, freq="1D")

        day_list = list(date_index.strftime('%d-%m-%Y'))


        for day in day_list:
            try:
                key = day
                timestamp = datetime.strptime(day, "%d-%m-%Y").timestamp() * 1000
                doc = {'day': day, 'timestamp': timestamp}
                self.coll_calendar.upsert(key, doc)
            except Exception as e:
                print(e)


def reset():
    cvc = CouchVeronaCard("couchtester", "couchtester", "verona_card", 'verona_card_0')
    cvc.connect()

    cvc.DropCollections()
    cvc.CreateCollections()
    cvc.IndexCollections()


def load():
    cvc = CouchVeronaCard("couchtester", "couchtester", "verona_card", 'verona_card_0')
    cvc.connect()

    basepath = "./dataset_veronacard_2014_2020/"
    csvfiles = glob.glob(basepath+'*.csv')
    #csvfiles = ["./dataset_veronacard_2014_2020/dati_2014.csv"]
    for csvfile in csvfiles:
        print(f"Processing: {csvfile}")
        logs = cvc.logGetData(csvfile)
        if len(logs)>0:
            print(f"Skippings: {csvfile}")
            continue
        cvc.importCSV(csvfile)
        cvc.logAddData(csvfile)

    #cvc.createPoiList()
    #cvc.createCalendar(2014)


def aggregate():
    cvc = CouchVeronaCard("couchtester", "couchtester", "verona_card", 'verona_card_0')
    cvc.connect()

    basepath = "./dataset_veronacard_2014_2020/"
    csvfiles = glob.glob(basepath+'*.csv')
    #csvfiles = ["./dataset_veronacard_2014_2020/dati_2014.csv"]
    for csvfile in csvfiles:
        print("AggregateDataFrom:" + csvfile)
        cvc.aggregateByVenueDate(csvfile)
        cvc.aggregateByCard(csvfile)

def query():
    cvc = CouchVeronaCard("couchtester", "couchtester", "verona_card", 'verona_card_0')
    cvc.connect()

    cvc.searchVenueLestVisitDay('17-08-2014')
    cvc.searchVenueMostMonthVisitYear('2014')
    cvc.searchCardsSwipeMultipleDays()


def main():
    #reset()
    #load()
    #aggregate()
    query()

if __name__ == '__main__':
    main()
