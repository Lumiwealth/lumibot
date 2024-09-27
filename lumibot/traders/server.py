import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKey, APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN
import requests

class LumibotServer:
    bot_server_api_key = ""
    bot_server_api_key_name = ""

    def __init__(self, trader, server_config):
        self.trader = trader
        self.server_config = server_config

        if isinstance(server_config, dict) and "BOT_SERVER_API_KEY" in server_config:
            self.bot_server_api_key = server_config["BOT_SERVER_API_KEY"]
        elif hasattr(server_config, "BOT_SERVER_API_KEY"):
            self.bot_server_api_key = server_config.BOT_SERVER_API_KEY
        else:
            raise ValueError("BOT_SERVER_API_KEY not found in config")
    
        if isinstance(server_config, dict) and "BOT_SERVER_API_KEY_NAME" in server_config:
            self.bot_server_api_key_name = server_config["BOT_SERVER_API_KEY_NAME"]
        elif hasattr(server_config, "BOT_SERVER_API_KEY_NAME"):
            self.bot_server_api_key_name = server_config.BOT_SERVER_API_KEY_NAME
        else:
            raise ValueError("BOT_SERVER_API_KEY not found in config")

    async def get_api_key(self, api_key: str = Security(APIKeyHeader(name="X-API-Key", auto_error=False))):
        if api_key == self.bot_server_api_key:
            return api_key
        else:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
            )

    def start_server(self):
        app = FastAPI()
        self.bots = self.trader.run_all_async()

        origins = [
            f"http://localhost:80",
        ]

        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )


        @app.get("/")
        def read_root():
            return {"message": "This is a Trading Bot Server!"}

        @app.get("/return_history")
        def return_history(api_key: str = Depends(self.get_api_key)):
            strategy = self.bots["executors"][0]
            historical_account_values = strategy.get_historical_bot_stats()
            historical_account_values["datetime"] = historical_account_values.index
            historical_data_dict = historical_account_values.to_dict(orient="records")

            return historical_data_dict


        @app.get("/bot_stats")
        def bot_stats(api_key: str = Depends(self.get_api_key)):
            # TODO: implement bot_stats
            return {"message": "This is Bot Stat!"}


        @app.post("/parameters")
        def update_parameters(parameters: dict, api_key: str = Depends(self.get_api_key)):
            strategy = self.bots["executors"][0]

            strategy.update_parameters(parameters)

            return {"status": "success", "parameters": strategy.parameters}
        
        
        uvicorn.run(app, host="127.0.0.1", port=8080)
        