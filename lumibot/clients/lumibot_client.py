import json
import os
from datetime import datetime
from functools import wraps
from inspect import signature

from flask import Flask

from lumibot import OrderStatus

from .blueprints import register_blueprints
from .config import AppConfig
from .extensions import db, register_extensions, register_user_datastore, sockets
from .models import (
    Log,
    Order,
    PortfolioPoint,
    Position,
    Role,
    Stats,
    Strategy,
    User,
    UserRole,
)
from .schemas import AssetSchema, LoggingSchema, PortfolioPointSchema
from .sockets import namespace


class LumibotClient:
    @staticmethod
    def default_serializer(o):
        if isinstance(o, datetime):
            return o.isoformat()

    def __init__(
        self, db_path, new_db=True, superadmin="lumiwealth", password="lumibot"
    ):
        # Setting main components
        # Setting global parameters
        self.app = Flask(__name__)
        AppConfig.set_database_uri(db_path)
        self.app.config.from_object(AppConfig)

        self.db = db
        self.db_path = db_path
        self.new_db = new_db
        self.default_superadmin = superadmin
        self.default_password = password
        self.socketio = sockets

        register_extensions(self.app)
        register_user_datastore(self.app, User, Role)
        register_blueprints(self.app)

        self.init_db()

    def emit_message(self, event, payload):
        for k, v in payload.items():
            if isinstance(v, datetime):
                payload[k] = v.__str__()
        self.socketio.emit(event, payload, namespace=namespace)

    def intercept(self, signal_name, source, type="result"):
        def wrapper(func):
            @wraps(func)
            def func_output(*args, **kwargs):
                if type == "input":
                    sig = signature(func)
                    bound_args = sig.bind(*args, **kwargs)

                    args_map = {}
                    for name, value in bound_args.arguments.items():
                        if name == "kwargs":
                            for k, v in value.items():
                                args_map[k] = v
                        else:
                            args_map[name] = value

                    for name, value in sig.parameters.items():
                        if name not in args_map:
                            args_map[name] = value.default

                    self.process_signal(signal_name, source, args_map)

                result = func(*args, **kwargs)

                if type == "result":
                    self.process_signal(signal_name, source, result)

                return result

            return func_output

        return wrapper

    def process_signal(self, signal_name, source, data=None):
        if signal_name == "log":
            payload = self.process_log_signal(source, data)
            self.emit_message("log", payload)
        elif signal_name == "portfolio_update":
            payload = self.process_portfolio_update_signal(source, data)
            self.emit_message("portfolio_update", payload)
        elif signal_name == "trace_stats":
            payload = self.process_trace_stats_signal(source, data)
            self.emit_message("trace_stats", payload)
        elif signal_name == "new_order":
            self.process_new_order_signal(source, data)
        elif signal_name == "canceled_order":
            self.process_canceled_order_signal(source, data)
        elif signal_name == "partially_filled_order":
            self.process_partially_filled_order_signal(source, data)
        elif signal_name == "filled_order":
            self.process_filled_order_signal(source, data)
        else:
            pass

    def process_new_strategy_signal(self, source):
        with self.app.app_context():
            Strategy.create(name=source)

    def process_portfolio_update_signal(self, source, data):
        with self.app.app_context():
            strategy = Strategy.get_by_name(source)
            total = data["portfolio_value"]
            unspent_money = data["unspent_money"]
            point = PortfolioPoint.create(
                strategy=strategy, unspent_money=unspent_money, total=total
            )
            portfolio_schema = PortfolioPointSchema()
            payload = portfolio_schema.dump(point)

        return payload

    def process_trace_stats_signal(self, source, data):
        with self.app.app_context():
            strategy = Strategy.get_by_name(source)
            j = json.dumps(data, default=self.default_serializer)
            Stats.create(strategy=strategy, json=j)

        return data

    def process_log_signal(self, source, data):
        with self.app.app_context():
            message = data.get("message")
            level = data.get("level")
            positions = data.get("positions")
            strategy = Strategy.get_by_name(source)

            log = Log.create(message=message, level=level, strategy=strategy)
            schema = LoggingSchema()
            payload = schema.dump(log)

            assets_schema = AssetSchema(many=True)
            assets = assets_schema.dump(positions)
            payload["assets"] = assets

        return payload

    def process_new_order_signal(self, source, data):
        with self.app.app_context():
            strategy = Strategy.get_by_name(source)
            order = data.get("order")
            identifier = order.identifier
            symbol = order.symbol
            quantity = order.quantity
            side = order.side
            status = OrderStatus.new_order
            Order.create(
                identifier=identifier,
                symbol=symbol,
                quantity=quantity,
                side=side,
                strategy=strategy,
                status=status,
                object=order._raw,
            )

    def process_canceled_order_signal(self, source, data):
        with self.app.app_context():
            order = data.get("order")
            stored_order = Order.get_order(source, order.identifier)
            stored_order.update(raw=order, status=OrderStatus.canceled_order)

    def process_partially_filled_order_signal(self, source, data):
        with self.app.app_context():
            order = data.get("order")
            stored_order = Order.get_order(source, order.identifier)
            stored_order.update(raw=order, status=OrderStatus.partially_filled_order)

    def process_filled_order_signal(self, source, data):
        with self.app.app_context():
            order = data.get("order")
            stored_order = Order.get_order(source, order.identifier)
            stored_order.update(raw=order, status=OrderStatus.filled_order)
            position = data.get("position")
            stored_position = Position.get_position(source, order.symbol)
            if stored_position:
                if position.quantity == 0:
                    stored_position.delete()
                else:
                    stored_position.update(quantity=position.quantity, raw=position)
            else:
                strategy = Strategy.get_by_name(source)
                Position.create(
                    symbol=order.symbol,
                    quantity=order.quantity,
                    strategy=strategy,
                    raw=position,
                )

    def create_superadmin(self):
        user = User.create(
            email=self.default_superadmin, password=self.default_password
        )
        role = Role.create(name="superadmin", description="has all access")
        UserRole.create(user_id=user.id, role_id=role.id)

    def init_db(self):
        with self.app.app_context():
            if self.new_db:
                try:
                    os.remove(self.db_path)
                except FileNotFoundError:
                    pass

                self.db.create_all()

            self.create_superadmin()

    def run(self, port="5000"):
        return self.socketio.run(self.app, port=port)
