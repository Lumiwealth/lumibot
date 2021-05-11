
from marshmallow import EXCLUDE, fields

from .extensions import ma
from .models import Log, PortfolioPoint


class LoggingSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Log


class PortfolioPointSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = PortfolioPoint


class AssetSchema(ma.Schema):
    class Meta:
        unknown = EXCLUDE

    symbol = ma.String()
    quantity = ma.Float()
    price = ma.Float()
