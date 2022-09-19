from CVC import CVC
from couchbase.options import QueryOptions

class CVCQuery(CVC):

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
            self.printError(e)

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
            self.printError(e)

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
            self.printError(e)



