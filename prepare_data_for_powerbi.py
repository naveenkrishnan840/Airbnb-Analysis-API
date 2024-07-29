import os

from pymongo import MongoClient
import pymongoarrow.monkey
import pandas as pd

pymongoarrow.monkey.patch_all()
client = MongoClient(os.getenv("MONGODB_URL"))

db = client.get_database("airbnb_analysis")

main_det = db.get_collection("airbnb_main_data").aggregate_pandas_all([
    {"$addFields": {"minimum_nights": {"$toInt": "$minimum_nights"}, "maximum_nights": {"$toInt": "$maximum_nights"},
                    "id": {"$toInt": "$_id"}}},
    {"$project": {"id": 1, "name": 1, "property_type": 1, "room_type": 1, "bed_type": 1, "minimum_nights": 1, "maximum_nights": 1, 
                  "cancellation_policy": 1, "bedrooms": 1, "accommodates": 1, "bedrooms": 1, "beds": 1, "bathrooms": 1, 
                  "number_of_reviews": 1, "price": 1, "_id": 0}}
])


host_details = db.get_collection("airbnb_main_data").aggregate_pandas_all([
    {"$project": {"host": 1, "_id": 0}}
]).to_dict(orient="records")


_id = db.get_collection("airbnb_main_data").aggregate_pandas_all([
    {"$addFields": {"id": {"$toInt": "$_id"}}},
    {"$project": {"id": 1, "_id": 0}}
])


sel_host_details = pd.DataFrame([value for host in host_details for key, value in host.items()],
                                columns=["host_name", "host_response_time", "host_response_rate", "host_is_superhost"])


sel_host_details["host_is_superhost"] = sel_host_details["host_is_superhost"].apply(lambda x: 1 if x else 0)

sel_host_details["id"] = _id


review_scores = db.get_collection("airbnb_main_data").aggregate_pandas_all([
    {"$project": {"review_scores": 1, "_id": 0}}
]).to_dict(orient="records")

review_scores = pd.DataFrame([value for review in review_scores for key, value in review.items()])

review_scores["id"] = _id

csv_list = main_det.join(sel_host_details, how="left", rsuffix="_left").join(review_scores, how="left", rsuffix="_left")

csv_list.drop(["id_left"], axis=1, inplace=True)

csv_list.to_csv("./Airbnb-Analysis.csv")
