import pymongo
import datetime

connection = pymongo.MongoClient()
collection = connection.e1_automobiles.cheapened_autos


def create(automobile_id, fell_by):
    """Создаёт запись в коллекции cheapened_autos

    :param automobile_id: id автомобиля
    :param fell_by: значение на сколько подешевел автомобиль
    """
    time_create = datetime.datetime.now()
    collection.insert({
        'automobile': automobile_id,
        'fell_by': fell_by,
        'created_at': time_create
    })
