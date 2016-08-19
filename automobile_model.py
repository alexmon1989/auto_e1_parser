import pymongo
import pymongo.errors
import time

connection = pymongo.MongoClient()
collection = connection.e1_automobiles.automobiles


def create(e1_id, seller, props):
    time_create = time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        collection.insert({
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

