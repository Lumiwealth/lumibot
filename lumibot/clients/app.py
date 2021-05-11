from .lumibot_client import LumibotClient

db_path = "dummy.db"
new_db = False

wrapper = LumibotClient(db_path, new_db=new_db)
app = wrapper.app
