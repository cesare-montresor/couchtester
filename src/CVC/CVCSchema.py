from datetime import timedelta, date, datetime

from CVC import CVC
from couchbase.options import QueryOptions

import pandas as pd

class CVCSchema(CVC):
    def __init__(self):
        super().__init__()

    def DropCollections(self):
        print("DropCollections ")
        qry_delete = "DROP COLLECTION {}.{}.{} IF EXISTS"
        try:
            for coll_name in self.coll_list:
                print("DropCollections: " + coll_name)
                opt = QueryOptions(timeout=timedelta(minutes=10))
                qry = self.cluster.query(qry_delete.format(self.bucket_name, self.scope_name, coll_name), opt)
                qry.execute()
        except Exception as e:
            print("ERROR: DropCollections: ")
            self.printError(e)

    def CreateCollections(self):
        qry_create = "CREATE COLLECTION {}.{}.{} IF NOT EXISTS"

        try:
            for coll_name in self.coll_list:
                print("CreateCollections: " + coll_name)
                # COLLECTION
                qry = self.cluster.query(qry_create.format(self.bucket_name, self.scope_name, coll_name))
                qry.execute()
        except Exception as e:
            print("ERROR: CreateCollections: ")
            self.printError(e)

    def IndexCollections(self):
        qry_index = "CREATE PRIMARY INDEX IF NOT EXISTS ON {}.{}.{}"

        custom_idx = [
            "create index idx_import_venue_name IF NOT EXISTS ON verona_card.verona_card_0.import(venue_name)",
            "create index idx_import_filename IF NOT EXISTS ON verona_card.verona_card_0.import(filename)",
            "create index idx_import_card_id IF NOT EXISTS ON verona_card.verona_card_0.import(card_id)",

            "create index idx_vc_poi_month IF NOT EXISTS ON verona_card.verona_card_0.vc_poi(month)",
            "create index idx_vc_poi_year IF NOT EXISTS ON verona_card.verona_card_0.vc_poi(year)",
            "create index idx_vc_poi_cnt IF NOT EXISTS ON verona_card.verona_card_0.vc_poi(cnt)",

            "create index idx_vc_card_num_days IF NOT EXISTS  ON verona_card.verona_card_0.vc_card(num_days)",
            "create index idx_vc_card_cnt IF NOT EXISTS  ON verona_card.verona_card_0.vc_card(cnt)"
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
                qry = self.cluster.query(qry_idx)
                qry.execute()

        except Exception as e:
            print("ERROR: IndexCollections: ")
            self.printError(e)

    def createPoiList(self):
        qry = "select distinct i.venue_name from verona_card.verona_card_0.import i"
        try:
            row_iter = self.cluster.query(qry)
            for row in row_iter:
                try:
                    key = row['venue_name']
                    self.coll_poi_list.upsert(key, row)
                except Exception as e:
                    self.printError(e)
        except Exception as e:
            self.printError(e)

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
                self.printError(e)


