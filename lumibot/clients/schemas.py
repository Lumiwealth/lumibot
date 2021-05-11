import json

from marshmallow import EXCLUDE, fields

from .extensions import ma
from .models import Log, PortfolioPoint, Stats, Strategy


class JsonField(fields.Field):
    def _serialize(self, value, attr, obj, **kwargs):
        value = json.loads(value)
        return value


class StrategySchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Strategy


class FilterQuerySchema(ma.Schema):
    class Meta:
        unknown = EXCLUDE

    limit = ma.Integer()


class LoggingSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Log


class AssetSchema(ma.Schema):
    class Meta:
        unknown = EXCLUDE

    symbol = ma.String()
    quantity = ma.Float()
    price = ma.Float()


class PortfolioPointSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = PortfolioPoint


class PortfolioDetailSchema(PortfolioPointSchema):
    positions = ma.List(ma.Nested(AssetSchema))


class StatsSchema(ma.SQLAlchemyAutoSchema):
    class Meta:
        model = Stats

    data = JsonField()
