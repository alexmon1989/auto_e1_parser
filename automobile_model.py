import pymongo.errors
import datetime
from connection import connection

collection = connection.e1_automobiles.automobiles


def create(e1_id, seller, props):
    time_create = datetime.datetime.now()
    try:
        return collection.insert({
            'e1_id': e1_id,
            'seller': seller,
            'props': props,
            'created_at': time_create,
            'updated_at': time_create
        })
    except pymongo.errors.DuplicateKeyError:
        pass


def get_by_e1_id(e1_id):
    auto = collection.find({'e1_id': e1_id})
    if auto.count() > 0:
        return auto[0]
    return None


def get_by_id(automobile_id):
    auto = collection.find({'_id': automobile_id})
    if auto.count() > 0:
        return auto[0]
    return None
