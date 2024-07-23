from fastapi import FastAPI, Response, Request, HTTPException, APIRouter, Depends
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import pymongoarrow.monkey

from database_connection import get_db_session
from request_frame import GetFilterRecord, GetDetails

app = FastAPI(title="Airbnb Analysis")
app.add_middleware(middleware_class=CORSMiddleware, allow_origins=["*"], allow_headers=["*"], allow_methods=["*"])

router = APIRouter()


def get_condition(getFilterRecord):
    db_condition = {"$and": []}
    property_type_condition = {"property_type": {"$in": getFilterRecord.property}}
    db_condition["$and"].append(property_type_condition)
    room_type_condition = {"room_type": {"$in": getFilterRecord.rooms}}
    db_condition["$and"].append(room_type_condition)
    no_of_bed_rooms = {}
    no_of_bed = {}
    no_of_bath_rooms = {}
    if not getFilterRecord.room_num == "Any":
        if getFilterRecord.room_num == "8+":
            no_of_bed_rooms["bedrooms"] = {"$gte": 8}
        else:
            no_of_bed_rooms["bedrooms"] = getFilterRecord.room_num
        db_condition["$and"].append(no_of_bed_rooms)
    if not getFilterRecord.bed_num == "Any":
        if getFilterRecord.bed_num == "8+":
            no_of_bed["beds"] = {"$gte": 8}
        else:
            no_of_bed["beds"] = getFilterRecord.bed_num
        db_condition["$and"].append(no_of_bed)
    if not getFilterRecord.bath_room_num == "Any":
        if getFilterRecord.bath_room_num == "8+":
            no_of_bath_rooms["bathrooms"] = {"$gte": 8}
        else:
            no_of_bath_rooms["bathrooms"] = getFilterRecord.bath_room_num
        db_condition["$and"].append(no_of_bath_rooms)

    amenities = {"$or": []}
    if getFilterRecord.essential_amenities:
        for i in getFilterRecord.essential_amenities:
            # ess_amenities = {"$setEquals": ["$amenities", getFilterRecord.essential_amenities]}
            amenities["$or"].append({"amenities": {"$in": [i]}})
    if getFilterRecord.features_amenities:
        for i in getFilterRecord.features_amenities:
            # fea_amenities = {"$setEquals": ["$amenities", getFilterRecord.features_amenities]}
            amenities["$or"].append({"amenities": {"$in": [i]}})
    if getFilterRecord.location_amenities:
        for i in getFilterRecord.location_amenities:
            # loc_amenities = {"$setEquals": ["$amenities", getFilterRecord.location_amenities]}
            amenities["$or"].append({"amenities": {"$in": [i]}})
    if getFilterRecord.safety_amenities:
        for i in getFilterRecord.safety_amenities:
            # safe_amenities = {"$setEquals": ["$amenities", getFilterRecord.safety_amenities]}
            amenities["$or"].append({"amenities": {"$in": [i]}})
    if amenities["$or"]:
        db_condition["$and"].append(amenities)

    if getFilterRecord.price_range:
        db_condition["$and"].append({"price": {"$gte": getFilterRecord.price_range[0]}})
        db_condition["$and"].append({"price": {"$lte": getFilterRecord.price_range[1]}})
    return db_condition


@router.post("/getFilterRecords")
# @router.on_event("startup")
def get_filter_records(request: Request, getFilterRecord: GetFilterRecord,
                       connection_details: MongoClient = Depends(get_db_session)):
    try:
        connection = connection_details
        database_connection = connection.get_database(name="airbnb_analysis")
        collection_name = database_connection.get_collection("airbnb_main_data")

        pymongoarrow.monkey.patch_all()
        db_condition = get_condition(getFilterRecord)

        docs_count = int(collection_name.aggregate_pandas_all(
            [{"$match": db_condition}, {"$project": {"_id": 1}}]).count()[0])

        df = collection_name.aggregate_pandas_all([
            {
                "$addFields": {
                    "price": {"$toInt": "$price"},
                    "review_ratings": {"$toInt": "$review_rating"},
                    # "minimum_night_price": {"$multiply": [{"$toInt": "$price"}, {"$toInt": "$minimum_nights"}]},
                    # "maximum_night_price": {"$multiply": [{"$toInt": "$price"}, {"$toInt": "$maximum_nights"}]},
                    "name_country": {"$concat": ["$name", ", ", "$address.country"]},
                    "thumbnail_url": "$images.picture_url", "review_rating": "$review_scores.review_scores_rating"
                }
            },
            {"$match": db_condition},
            {"$project": {"name_country": 1, "thumbnail_url": 1, "minimum_nights": 1, "maximum_nights": 1, "price": 1,
                          # "minimum_night_price": 1,
                          # "maximum_night_price": 1,
                          # "review_rating": 1
                          }
             },
            {"$limit": getFilterRecord.limit}
        ])
        # Property Type group aggregation 
        property_type_group_aggregation = collection_name.aggregate_pandas_all(
            [
                # {"$addFields": {"minimum_nights": {"$toInt": "$minimum_nights"},
                #                 "maximum_nights": {"$toInt": "$maximum_nights"}}},
                {"$addFields": {"price": {"$toInt": "$price"}}},
                {"$match": db_condition},
                {"$group": {"_id": "$property_type", "minimum_price": {"$min": "$price"},
                            "maximum_price": {"$max": "$price"}}},
                {"$project": {"_id": 1, "minimum_price": 1, "maximum_price": 1}},
                {"$sort": {"property_type": 1}}
            ]
        )
        # Country Wise Avg Price
        country_wise_avg_price = collection_name.aggregate_pandas_all(
            [
                {"$addFields": {"price": {"$toInt": "$price"}}},
                {"$match": db_condition},
                {"$group": {"_id": "$address.country", "avg_price": {"$avg": "$price"}}},
                {"$project": {"avgPrice": {"$toInt": {"$round": "$avg_price"}}}},
                {"$sort": {"avg_price": -1}}
            ]
        )
        # Room Type group aggregation
        room_type_group_aggregation = collection_name.aggregate_pandas_all(
            [
                {"$addFields": {"price": {"$toInt": "$price"}}},
                {"$match": db_condition},
                {"$group": {"_id": "$room_type", "minimum_price": {"$min": "$price"},
                            "maximum_price": {"$max": "$price"}}},
                {"$sort": {"property_type": 1}}
            ]
        )
        # Property Type Wise No of Reviews
        property_type_wise_no_of_reviews = collection_name.aggregate_pandas_all(
            [
                {"$match": db_condition},
                {"$group": {"_id": "$property_type", "no_of_reviews": {"$sum": "$number_of_reviews"}}}
            ]
        )
        result = {
            "docs_count": docs_count,
            "room_type_group_aggregation": room_type_group_aggregation.to_dict(orient="records"),
            "country_wise_avg_price": country_wise_avg_price.to_dict(orient="records"),
            "property_type_group_aggregation": property_type_group_aggregation.to_dict(orient="records"),
            "property_type_wise_no_of_reviews": property_type_wise_no_of_reviews.to_dict(orient="records"),
            "df": df.to_dict(orient="records") if not df.empty else []
        }
        print(df)
        print(connection)
        print(getFilterRecord)
        return HTTPException(status_code=200, detail=result)
    except Exception as e:
        raise e


@router.post("/showMoreRecords")
def show_more_records(request: Request, getFilterRecord: GetFilterRecord,
                      connection_details: MongoClient = Depends(get_db_session)):
    try:
        connection = connection_details
        database_connection = connection.get_database(name="airbnb_analysis")
        collection_name = database_connection.get_collection("airbnb_main_data")

        pymongoarrow.monkey.patch_all()
        db_condition = get_condition(getFilterRecord)
        df = collection_name.aggregate_pandas_all([
            {
                "$addFields": {
                    "price": {"$toInt": "$price"},
                    "review_ratings": {"$toInt": "$review_rating"},
                    # "minimum_night_price": {"$multiply": [{"$toInt": "$price"}, {"$toInt": "$minimum_nights"}]},
                    # "maximum_night_price": {"$multiply": [{"$toInt": "$price"}, {"$toInt": "$maximum_nights"}]},
                    "name_country": {"$concat": ["$name", ", ", "$address.country"]},
                    "thumbnail_url": "$images.picture_url", "review_rating": "$review_scores.review_scores_rating"
                }
            },
            {"$match": db_condition},
            {"$project": {"name_country": 1, "thumbnail_url": 1, "minimum_nights": 1, "maximum_nights": 1, "price": 1,
                          # "minimum_night_price": 1,
                          # "maximum_night_price": 1,
                          # "review_rating": 1
                          }
             },
            {"$limit": getFilterRecord.limit}
        ])
        return HTTPException(status_code=200, detail=df.to_dict(orient="records") if not df.empty else [])
    except Exception as e:
        raise e


@router.post("/getDetails")
def get_details(request: Request, getDetails: GetDetails, connection_details: MongoClient = Depends(get_db_session)):
    try:
        connection = connection_details
        database_connection = connection.get_database(name="airbnb_analysis")
        collection_name = database_connection.get_collection("airbnb_main_data")
        get_data_details = collection_name.find_pandas_all(
            {"_id": getDetails.doc_id}
        )
        return HTTPException(status_code=200, detail=get_data_details.to_dict(orient="records"))
    except Exception as e:
        raise e


app.include_router(router=router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8001)
