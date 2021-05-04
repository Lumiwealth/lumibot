from flask import Blueprint, render_template
from flask_security import login_required

main_views = Blueprint("main_views", __name__)


@main_views.route("/")
@login_required
def index():
    return render_template("pages/dashboard.html")


def register_blueprints(app):
    app.register_blueprint(main_views)
