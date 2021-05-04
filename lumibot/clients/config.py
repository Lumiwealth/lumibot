import os


class AppConfig:
    # Global parameters
    SECRET_KEY = "lumibot"

    # Frontend parameters
    TEMPLATES_AUTO_RELOAD = True

    # Database parameters
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_DATABASE_URI = f"sqlite:///:memory:"

    # Security parameters
    SECURITY_PASSWORD_SALT = "ATGUOHAELKiubahiughaerGOJAEGj"
    SECURITY_LOGIN_USER_TEMPLATE = "pages/login_user.html"

    @classmethod
    def set_database_uri(cls, db_path):
        db_path = os.path.join(os.getcwd(), db_path)
        dirname = os.path.dirname(db_path)
        if not os.path.exists(dirname):
            os.mkdir(dirname)

        cls.SQLALCHEMY_DATABASE_URI = f"sqlite:///{db_path}"
