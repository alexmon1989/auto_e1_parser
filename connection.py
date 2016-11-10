from pymongo import MongoClient
import configparser
import os

# Соединение с MongoDB
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'settings.ini'))
connection = MongoClient(host=config['DB']['Host'], port=int(config['DB']['Port']))
