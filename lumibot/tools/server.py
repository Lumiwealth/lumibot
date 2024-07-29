import json
import os
import sys
import logging
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKey, APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN

# TODO: Clean this up
ENV = os.environ.get("ENV")
PORT = os.environ.get("PORT")
OWNER_ID = os.environ.get("OWNER_ID")
STARTING_PARAMETERS = json.loads(os.environ.get("STARTING_PARAMETERS"))
API_KEY = os.environ.get("API_KEY")
API_KEY_NAME = "Authorization"
BROKER = os.environ.get("BROKER")
API_DETAILS = json.loads(os.environ.get("API_DETAILS"))

class LumibotServer:
    api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
    
    def __init__(self, bot):
        self.bot = bot

    async def get_api_key(
        api_key_header: str = Security(api_key_header),
    ):
        if api_key_header == API_KEY:
            return api_key_header
        else:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
            )

    def start_server():
        app = FastAPI()

        origins = [
            f"http://localhost:{PORT}",
        ]


        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

        @app.get("/")
        def read_root():
            return {"Hello": "World"}


        @app.get("/return_history")
        def return_history(api_key: APIKey = Depends(get_api_key)):
            strategy = bot["executors"][0]

            historical_account_values = strategy.get_historical_bot_stats()
            historical_account_values["datetime"] = historical_account_values.index
            historical_data_dict = historical_account_values.to_dict(orient="records")

            return historical_data_dict


        @app.get("/bot_stats")
        def bot_stats(api_key: APIKey = Depends(get_api_key)):
            return get_bot_stats(bot)


        @app.post("/parameters")
        def update_parameters(parameters: dict, api_key: APIKey = Depends(get_api_key)):
            strategy = self.bot["executors"][0]

            strategy.update_parameters(parameters)

            # TODO: Add error handling

            return {"status": "success", "parameters": strategy.parameters}
        
        uvicorn.run(app, host="0.0.0.0", port=int(PORT))