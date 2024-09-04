import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Security
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKey, APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN

class LumibotServer:
    def __init__(self, bot, server_config):
        self.bot = bot
        self.server_config = server_config

        if isinstance(server_config, dict) and "PORT" in server_config:
            self.server_port = server_config["PORT"]
        elif hasattr(server_config, "PORT"):
            self.server_port = server_config.PORT
        else:
            raise ValueError("SERVER_PORT not found in config")

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
   
        self.api_key_header = APIKeyHeader(name=self.bot_server_api_key_name, auto_error=False)

    async def get_api_key(self, api_key_header: str = Security(api_key_header),
    ):
        if api_key_header == bot_server_api_key:
            return api_key_header
        else:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
            )

    def start_server(self):
        app = FastAPI()

        origins = [
            f"http://localhost:{server_port}",
        ]


        app.add_middleware(
            CORSMiddleware,
            allow_origins=origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # api_key_header = APIKeyHeader(name=self.bot_server_api_key_name, auto_error=False)

        @app.get("/")
        def read_root():
            return {"message": "This is a Trading Bot Server!"}

        @app.get("/return_history")
        def return_history(api_key: APIKey = Depends(self.bot_server_api_key)):
            strategy = self.bot["executors"][0]

            historical_account_values = strategy.get_historical_bot_stats()
            historical_account_values["datetime"] = historical_account_values.index
            historical_data_dict = historical_account_values.to_dict(orient="records")

            return historical_data_dict


        @app.get("/bot_stats")
        def bot_stats(api_key: APIKey = Depends(self.bot_server_api_key)):
            return get_bot_stats(bot)


        @app.post("/parameters")
        def update_parameters(parameters: dict, api_key: APIKey = Depends(self.bot_server_api_key)):
            strategy = self.bot["executors"][0]

            strategy.update_parameters(parameters)

            # TODO: Add error handling

            return {"status": "success", "parameters": strategy.parameters}
        
        uvicorn.run(app, host="0.0.0.0", port=int(self.server_port))