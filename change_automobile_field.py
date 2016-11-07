import pymongo
import automobile_model


if __name__ == '__main__':
    connection = pymongo.MongoClient()
    collection = connection.e1_automobiles.prices

    prices_ids = []

    for price in collection.find():
        automobile = automobile_model.get_by_id(price['automobile'])
        if automobile:
            print('Обновляем: ', price['_id'])
            props = dict()
            props['manufacturer'] = automobile['props']['manufacturer']
            props['model'] = automobile['props']['model']
            props['year'] = automobile['props']['Год выпуска']
            props['transmission'] = automobile['props'].get('КПП')
            collection.update({'_id': price['_id']}, {"$set": {'automobile_props': props}})
