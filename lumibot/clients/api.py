from flask.views import MethodView
from flask_smorest import Blueprint

from .models import Log, PortfolioPoint, Position, Stats, Strategy
from .schemas import (
    FilterQuerySchema,
    LoggingSchema,
    PortfolioDetailSchema,
    PortfolioPointSchema,
    StatsSchema,
    StrategySchema,
)

data_blp = Blueprint("Data", __name__)


@data_blp.route("/data/strategies/")
class Strategies(MethodView):
    @data_blp.response(200, StrategySchema(many=True))
    def get(self):
        query = Strategy.query
        results = query.all()
        return results


@data_blp.route("/data/strategies/<strategy_id>/logs/")
class Logs(MethodView):
    @data_blp.arguments(FilterQuerySchema, location="query")
    @data_blp.response(200, LoggingSchema(many=True))
    def get(self, args, strategy_id):

        limit = args.get("limit")
        if limit is None:
            limit = 100

        query = (
            Log.query.filter_by(strategy_id=strategy_id)
            .order_by(Log.time.desc())
            .limit(limit)
        )
        result = query.all()
        return reversed(result)


@data_blp.route("/data/strategies/<strategy_id>/evolution/")
class PortfolioEvolution(MethodView):
    @data_blp.arguments(FilterQuerySchema, location="query")
    @data_blp.response(200, PortfolioPointSchema(many=True))
    def get(self, args, strategy_id):

        limit = args.get("limit")
        if limit is None:
            limit = 100

        query = (
            PortfolioPoint.query.filter_by(strategy_id=strategy_id)
            .order_by(PortfolioPoint.time.desc())
            .limit(limit)
        )
        result = query.all()
        return reversed(result)


@data_blp.route("/data/strategies/<strategy_id>/portfolio/")
class Portfolio(MethodView):
    @data_blp.response(200, PortfolioDetailSchema)
    def get(self, strategy_id):
        positions = Position.query.all()
        query = (
            PortfolioPoint.query.filter_by(strategy_id=strategy_id)
            .order_by(PortfolioPoint.time.desc())
            .limit(1)
        )
        last_update = query.first()
        if last_update is None:
            return {}

        positions_details = []
        for position in positions:
            positions_details.append(
                {
                    "symbol": position.symbol,
                    "quantity": position.quantity,
                    "price": position.latest_price,
                }
            )
        last_update.positions = positions_details
        return last_update


@data_blp.route("/data/strategies/<strategy_id>/stats/")
class TraceStats(MethodView):
    @data_blp.arguments(FilterQuerySchema, location="query")
    @data_blp.response(200, StatsSchema(many=True))
    def get(self, args, strategy_id):
        limit = args.get("limit")
        if limit is None:
            limit = 100

        query = (
            Stats.query.filter_by(strategy_id=strategy_id)
            .order_by(Stats.time.desc())
            .limit(limit)
        )
        result = query.all()
        return reversed(result)


def register_api(app):
    app.register_blueprint(data_blp)
