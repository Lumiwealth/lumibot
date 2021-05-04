from datetime import datetime, timezone

from sqlalchemy.sql import expression

from .extensions import db


class CRUDMixin(object):
    @classmethod
    def create(cls, **kwargs):
        """Create a new record and save it the database."""
        instance = cls(**kwargs)
        return instance.save()

    def update(self, commit=True, **kwargs):
        """Update specific fields of a record."""
        for attr, value in kwargs.items():
            setattr(self, attr, value)
        return commit and self.save() or self

    def save(self, commit=True):
        """Save the record."""
        db.session.add(self)
        if commit:
            db.session.commit()
        return self

    def delete(self, commit=True):
        """Remove the record from the database."""
        db.session.delete(self)
        return commit and db.session.commit()


class Model(CRUDMixin, db.Model):
    __abstract__ = True


class AwareDateTime(db.TypeDecorator):
    impl = db.DateTime

    def process_result_value(self, value, dialect):
        if value is not None:
            return value.replace(tzinfo=timezone.utc)

        return value


class TimeData(object):
    __table_args__ = {"extend_existing": True}

    time = db.Column(
        AwareDateTime(), default=lambda: datetime.now().astimezone(timezone.utc)
    )


class SurrogatePK(object):
    __table_args__ = {"extend_existing": True}

    id = db.Column(db.Integer, primary_key=True)

    @classmethod
    def get_by_id(cls, record_id):
        """Get record by ID."""
        if any(
            (
                isinstance(record_id, (str, bytes)) and record_id.isdigit(),
                isinstance(record_id, (int, float)),
            )
        ):
            return cls.query.get(int(record_id))
        return None


def reference_col(
    tablename, index=False, nullable=False, primary_key=False, pk_name="id", **kwargs
):
    return db.Column(
        db.ForeignKey(f"{tablename}.{pk_name}", **kwargs),
        index=index,
        nullable=nullable,
        primary_key=primary_key,
    )
