import datetime
from connection import connection

collection = connection.e1_automobiles.errors


def create(e1_id, error_type, http_error_code):
    collection.insert({
        'e1_id': e1_id,
        'type': error_type,
        'http_code': http_error_code,
        'created_at': datetime.datetime.now()
    })
