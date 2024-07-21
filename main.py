from fastapi import FastAPI, Response, Request, HTTPException, APIRouter, Depends
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import pymongoarrow.monkey

from database_connection import get_db_session
from request_frame import GetFilterRecord

app = FastAPI(title="Airbnb Analysis")
app.add_middleware(middleware_class=CORSMiddleware, allow_origins=["*"], allow_headers=["*"], allow_methods=["*"])

router = APIRouter()


@router.post("/getFilterRecords")
# @router.on_event("startup")
def get_filter_records(request: Request, getFilterRecord: GetFilterRecord,
                       connection_details: MongoClient = Depends(get_db_session)):
    try:
        connection = connection_details
        database_connection = connection.get_database(name="airbnb_analysis")
        collection_name = database_connection.get_collection("airbnb_main_data")
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
        db_condition["$and"].append(amenities)

        if getFilterRecord.price_range:
            db_condition["$and"].append({"minimum_night_price": {"$gte": getFilterRecord.price_range[0]}})
            db_condition["$and"].append({"maximum_night_price": {"$lte": getFilterRecord.price_range[1]}})

        pymongoarrow.monkey.patch_all()
        df = collection_name.aggregate_pandas_all([
            {
                "$addFields": {
                    "price": {"$toInt": "$price"},
                    "review_ratings": {"$toInt": "$review_rating"},
                    "minimum_night_price": {"$multiply": [{"$toInt": "$price"}, {"$toInt": "$minimum_nights"}]},
                    "maximum_night_price": {"$multiply": [{"$toInt": "$price"}, {"$toInt": "$maximum_nights"}]},
                    "name_country": {"$concat": ["$name", ", ", "$address.country"]},
                    "thumbnail_url": "$images.picture_url", "review_rating": "$review_scores.review_scores_rating"
            }
            },
            {"$match": db_condition},
            {"$project": {"name_country": 1, "thumbnail_url": 1, "minimum_nights": 1, "maximum_nights": 1, "price": 1,
                          "minimum_night_price": 1, "maximum_night_price": 1,
                          # "review_rating": 1
                          }
            }
        ])
        result = df.to_dict(orient="records") if not df.empty else []
        print(df)
        print(connection)
        print(getFilterRecord)
        return HTTPException(status_code=200, detail=result)
    except Exception as e:
        raise e


app.include_router(router=router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)
