import pymongo
import datetime

connection = pymongo.MongoClient()
collection = connection.e1_automobiles.prices


def create(value, automobile_id):
    time_create = datetime.datetime.now()
    collection.insert({
        'value': value,
        'automobile': automobile_id,
        'created_at': time_create,
        'updated_at': time_create
    })


def get_last_auto_price(automobile_id):
    price = collection.find_one({'automobile': automobile_id}, {'value': 1}, sort=[("created_at", -1)])
    if price:
        return price['value']
