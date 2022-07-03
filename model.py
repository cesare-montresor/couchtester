class Model():
    def toDictionary(self):
        return self.__dict__()

class ModelCardSwipe(Model):
    def __init__(self, date, time, venue_name, venue_id, card_id, date_event, field1, field2, card_type):
        self.date = date
        self.time = time
        self.venue_name = venue_name
        self.venue_id = venue_id
        self.card_id = card_id
        self.date_event = date_event
        self.field1 = field1
        self.field2 = field2
        self.card_type = card_type

    def __dict__(self):
        return {
            "date": self.date,
            "time": self.time,
            "venue_name": self.venue_name,
            "venue_id": self.venue_id,
            "card_id": self.card_id,
            "date_event": self.date_event,
            "field1": self.field1,
            "field2": self.field2,
            "card_type": self.card_type,
        }