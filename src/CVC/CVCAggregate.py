from datetime import timedelta
from CVC import CVC
from couchbase.options import QueryOptions


class CVCAggregate(CVC):

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
                    self.printError(f"AggregateByVenueDate:write:\n {e}")
        except Exception as e:
            self.printError(f"AggregateByVenueDate:read:\n {e}")

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
                    self.printError(e)
        except Exception as e:
            self.printError(e)

