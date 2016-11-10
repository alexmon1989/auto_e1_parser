import datetime
from connection import connection

collection = connection.e1_automobiles.prices


def create(value, mileage, automobile_id, automobile_props):
    """Создаёт запись в коллекции prices

    :param value: Значение цены
    :param mileage: Значение пробега
    :param automobile_id: id автомобиля
    """
    time_create = datetime.datetime.now()
    collection.insert({
        'value': value,
        'mileage': mileage,
        'automobile': automobile_id,
        'automobile_props': automobile_props,
        'created_at': time_create,
        'updated_at': time_create
    })


def get_last_auto_price(automobile_id):
    price = collection.find_one({'automobile': automobile_id}, {'value': 1}, sort=[("created_at", -1)])
    if price:
        return price['value']
    return None
