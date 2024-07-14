from fastapi import FastAPI, Response, Request, HTTPException, APIRouter, Depends
from fastapi.middleware.cors import CORSMiddleware


from database_connection import get_db_session


app = FastAPI(title="Airbnb Analysis")
app.add_middleware(middleware_class=CORSMiddleware, allow_origins=["*"], allow_headers=["*"], allow_methods=["*"])

router = APIRouter()


@router.get("/show_filter_records")
# @router.on_event("startup")
def get_filter_records(request: Request, response: Response, connection: Depends(get_db_session())):
    print(connection)
    return HTTPException(status_code=200, detail=["Sucess"])


app.include_router(router=router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9001)











