from pydantic import BaseModel
from typing import List, AnyStr


class GetFilterRecord(BaseModel):
    property: list[str]
    rooms: list[str]
    room_num: str
    bed_num: str
    bath_room_num: str
    essential_amenities: list[str]
    features_amenities: list[str]
    location_amenities: list[str]
    safety_amenities: list[str]
    price_range: list[int]
    limit: int


class GetDetails(BaseModel):
    doc_id: str




