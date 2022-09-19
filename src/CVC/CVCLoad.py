from couchbase.options import QueryOptions
from datetime import timedelta, date, datetime
import csv
import time


from CVC import CVC

class CVCLoad(CVC):
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

    def loadCSV(self, filename):
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
                    self.printError(e)

    def logGetData(self, filename=None):
        qry = """
            SELECT l.*
            FROM verona_card.verona_card_0.vc_log l
        """
        if filename is not None:
            qry += "\nWHERE filename = $1"

        params = QueryOptions(positional_parameters=[filename])
        row_iter = self.cluster.query(qry, params)
        results = []
        try:
            for row in row_iter:
                print(row)
                results.append(row)
        except Exception as e:
            self.printError(e)
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
            self.printError(e)
            return False

        key = filename.replace(' ', '_')
        doc = {
            'filename': filename,
            'date_import': int(time.time() * 1000),
            'timestamp_min': result['timestamp_min'],
            'timestamp_max': result['timestamp_max']
        }
        try:
            print(result)
            print(doc)
            self.coll_log.upsert(key, doc)
        except Exception as e:
            self.printError(e)
            result = False
        return result



