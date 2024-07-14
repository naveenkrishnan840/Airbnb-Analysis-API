from pymongo import MongoClient


def get_db_session():
    uri = ("mongodb+srv://navaneethan:zZix8wAXMz5O87K4@airbnb-analysis.yiorqff.mongodb.net/?retryWrites=true&w="
           "majority&appName=Airbnb-Analysis")
    return MongoClient(host=uri)

