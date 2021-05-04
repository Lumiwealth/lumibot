import enum

from flask_security import RoleMixin, UserMixin
from sqlalchemy import and_

from .database import Model, SurrogatePK, TimeData, db


class UserRole(Model):
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), primary_key=True)
    role_id = db.Column(db.Integer, db.ForeignKey("role.id"), primary_key=True)

    user = db.relationship("User", backref=db.backref("user_roles"))
    role = db.relationship("Role", backref=db.backref("role_users"))


class Role(Model, SurrogatePK, RoleMixin):
    name = db.Column(db.String(80), unique=True)
    description = db.Column(db.String(255))

    def __str__(self):
        return self.name


class User(Model, SurrogatePK, UserMixin):
    email = db.Column(db.String(255), unique=True)
    password = db.Column(db.String(255))
    active = db.Column(db.Boolean(), default=True)


class Strategy(Model, SurrogatePK):
    name = db.Column(db.String(255), unique=True)

    logs = db.relationship("Log", backref=db.backref("strategy"))
    stats = db.relationship("Stats", backref=db.backref("strategy"))
    positions = db.relationship("Position", backref=db.backref("strategy"))
    orders = db.relationship("Order", backref=db.backref("strategy"))
    portfolio_points = db.relationship("PortfolioPoint", backref=db.backref("strategy"))

    @classmethod
    def get_by_name(cls, name):
        return cls.query.filter_by(name=name).first()

    def __str__(self):
        return self.name


class Log(Model, SurrogatePK, TimeData):
    message = db.Column(db.UnicodeText)
    level = db.Column(db.String(20))

    strategy_id = db.Column(db.Integer, db.ForeignKey("strategy.id"))

    def __str__(self):
        return f"{self.time} {self.level}: {self.message}"


class Stats(Model, SurrogatePK, TimeData):
    json = db.Column(db.JSON())

    strategy_id = db.Column(db.Integer, db.ForeignKey("strategy.id"))

    def __str__(self):
        return f"Strategy {self.strategy.name} stats row at {self.time}"


class Order(Model, SurrogatePK):
    class SideEnum(enum.Enum):
        sell = 0
        buy = 1

    identifier = db.Column(db.UnicodeText())
    symbol = db.Column(db.String(10))
    quantity = db.Column(db.Float())
    side = db.Column(db.Enum(SideEnum))
    status = db.Column(db.String(20))
    object = db.Column(db.PickleType())

    strategy_id = db.Column(db.Integer, db.ForeignKey("strategy.id"))

    @classmethod
    def get_order(cls, strategy_name, identifier):

        # orders = cls.query.filter_by(identifier=identifier).all()

        # order_id = db.session.query(cls.id).join(Strategy).filter(
        #     and_(Strategy.name == strategy_name, cls.identifier == identifier)
        # ).first()

        return (
            cls.query.join(Strategy)
            .filter(and_(Strategy.name == strategy_name, cls.identifier == identifier))
            .first()
        )

    def __str__(self):
        return f"Strategy {self.strategy.name} order: {self.side} {self.quantity} of {self.symbol}"


class Position(Model, SurrogatePK):
    symbol = db.Column(db.String(10))
    quantity = db.Column(db.Float())
    raw = db.Column(db.PickleType())

    strategy_id = db.Column(db.Integer, db.ForeignKey("strategy.id"))

    @classmethod
    def get_position(cls, strategy_name, symbol):
        return (
            cls.query.join(Strategy)
            .filter(and_(Strategy.name == strategy_name, cls.symbol == symbol))
            .first()
        )

    def __str__(self):
        return (
            f"Strategy {self.strategy.name} position: {self.quantity} of {self.symbol}"
        )


class PortfolioPoint(Model, SurrogatePK, TimeData):
    total = db.Column(db.Float())
    unspent_money = db.Column(db.Float())

    strategy_id = db.Column(db.Integer, db.ForeignKey("strategy.id"))

    def __str__(self):
        return f"Strategy {self.strategy.name} portfolio value: {self.total} at {self.time}"
