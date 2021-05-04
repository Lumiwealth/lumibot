from flask_marshmallow import Marshmallow
from flask_security import Security, SQLAlchemyUserDatastore
from flask_socketio import SocketIO
from flask_sqlalchemy import SQLAlchemy

sockets = SocketIO(logger=True, engineio_logger=True)
db = SQLAlchemy(session_options={"autocommit": False})
ma = Marshmallow()
security = Security()


def register_extensions(app):
    sockets.init_app(app)
    db.init_app(app)
    ma.init_app(app)


def register_user_datastore(app, User, Role):
    user_datastore = SQLAlchemyUserDatastore(db, User, Role)
    security_state = security.init_app(app, user_datastore)
    security._state = security_state
