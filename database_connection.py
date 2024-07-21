from pymongo import MongoClient
import pymongoarrow.monkey


def get_db_session():
    try:
        pymongoarrow.monkey.patch_all()
        uri = ("mongodb+srv://navaneethan:BgH6Qn03wRtmdGoR@airbnb-analysis.yiorqff.mongodb.net/?retryWrites=true&w="
               "majority&appName=Airbnb-Analysis")
        return MongoClient(host=uri)
    except Exception as e:
        raise e

