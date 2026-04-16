import datetime as dt
import hashlib
import math
from decimal import Decimal


class CashEvent:
    """Normalized broker cash activity payload for cloud telemetry."""

    VALID_EVENT_TYPES = {
        "deposit",
        "withdrawal",
        "interest",
        "dividend",
        "fee",
        "journal",
        "adjustment",
        "tax",
        "other_cash",
    }
    VALID_DIRECTIONS = {"in", "out", "neutral"}

    def __init__(
        self,
        *,
        broker_name: str,
        event_type: str,
        amount,
        occurred_at,
        event_id: str | None = None,
        broker_event_id: str | None = None,
        raw_type: str | None = None,
        raw_subtype: str | None = None,
        currency: str | None = "USD",
        description: str | None = None,
        direction: str | None = None,
        is_external_cash_flow: bool = False,
    ) -> None:
        normalized_broker_name = str(broker_name or "").strip().lower()
        if not normalized_broker_name:
            raise ValueError("broker_name is required")

        normalized_event_type = str(event_type or "").strip().lower()
        if normalized_event_type not in self.VALID_EVENT_TYPES:
            raise ValueError(
                f"event_type must be one of {sorted(self.VALID_EVENT_TYPES)}, got {event_type!r}"
            )

        normalized_amount = self.coerce_amount(amount)
        normalized_occurred_at = self.coerce_datetime(occurred_at)
        normalized_direction = str(direction or self._infer_direction(normalized_amount)).strip().lower()
        if normalized_direction not in self.VALID_DIRECTIONS:
            raise ValueError(
                f"direction must be one of {sorted(self.VALID_DIRECTIONS)}, got {direction!r}"
            )

        normalized_broker_event_id = self._clean_optional_str(broker_event_id)
        normalized_raw_type = self._clean_optional_str(raw_type)
        normalized_raw_subtype = self._clean_optional_str(raw_subtype)
        normalized_description = self._clean_optional_str(description)
        normalized_currency = self._clean_optional_str(currency)
        if normalized_currency is not None:
            normalized_currency = normalized_currency.upper()

        self.event_id = self._clean_optional_str(event_id) or self.build_event_id(
            broker_name=normalized_broker_name,
            broker_event_id=normalized_broker_event_id,
            raw_type=normalized_raw_type,
            raw_subtype=normalized_raw_subtype,
            occurred_at=normalized_occurred_at,
            amount=normalized_amount,
            description=normalized_description,
        )
        self.broker_event_id = normalized_broker_event_id
        self.broker_name = normalized_broker_name
        self.event_type = normalized_event_type
        self.raw_type = normalized_raw_type
        self.raw_subtype = normalized_raw_subtype
        self.amount = normalized_amount
        self.currency = normalized_currency
        self.occurred_at = normalized_occurred_at
        self.description = normalized_description
        self.direction = normalized_direction
        self.is_external_cash_flow = bool(is_external_cash_flow)

    @staticmethod
    def _clean_optional_str(value) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _infer_direction(amount: float) -> str:
        if amount > 0:
            return "in"
        if amount < 0:
            return "out"
        return "neutral"

    @staticmethod
    def coerce_amount(value) -> float:
        if value is None:
            return 0.0
        if isinstance(value, Decimal):
            value = float(value)
        elif isinstance(value, str):
            value = value.replace("$", "").replace(",", "").strip()
            if not value:
                return 0.0

        try:
            amount = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"amount must be numeric, got {value!r}") from exc

        if not math.isfinite(amount):
            raise ValueError(f"amount must be finite, got {value!r}")
        return amount

    @staticmethod
    def coerce_datetime(value) -> dt.datetime:
        if value is None:
            raise ValueError("occurred_at is required")

        if isinstance(value, dt.datetime):
            normalized = value
        elif isinstance(value, dt.date):
            normalized = dt.datetime.combine(value, dt.time.min, tzinfo=dt.timezone.utc)
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                raise ValueError("occurred_at string cannot be empty")
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                normalized = dt.datetime.fromisoformat(text)
            except ValueError:
                parsed = None
                for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
                    try:
                        parsed = dt.datetime.strptime(text, fmt)
                        break
                    except ValueError:
                        continue
                if parsed is None:
                    raise
                normalized = parsed
        else:
            raise ValueError(f"Unsupported occurred_at value {value!r}")

        if normalized.tzinfo is None or normalized.tzinfo.utcoffset(normalized) is None:
            normalized = normalized.replace(tzinfo=dt.timezone.utc)
        return normalized.astimezone(dt.timezone.utc)

    @classmethod
    def build_event_id(
        cls,
        *,
        broker_name: str,
        broker_event_id: str | None = None,
        raw_type: str | None = None,
        raw_subtype: str | None = None,
        occurred_at,
        amount,
        description: str | None = None,
        extra_components: list[str | None] | tuple[str | None, ...] | None = None,
    ) -> str:
        normalized_broker_name = str(broker_name or "").strip().lower() or "unknown"
        cleaned_broker_event_id = cls._clean_optional_str(broker_event_id)
        if cleaned_broker_event_id is not None:
            return f"{normalized_broker_name}:{cleaned_broker_event_id}"

        normalized_occurred_at = cls.coerce_datetime(occurred_at).isoformat()
        normalized_amount = cls.coerce_amount(amount)
        digest_source = "|".join(
            [
                normalized_broker_name,
                cls._clean_optional_str(raw_type) or "",
                cls._clean_optional_str(raw_subtype) or "",
                normalized_occurred_at,
                f"{normalized_amount:.12f}",
                cls._clean_optional_str(description) or "",
                *(
                    cls._clean_optional_str(component) or ""
                    for component in (extra_components or [])
                ),
            ]
        )
        digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:24]
        return f"{normalized_broker_name}:synthetic:{digest}"

    def to_dict(self) -> dict:
        result = {
            "event_id": self.event_id,
            "broker_name": self.broker_name,
            "event_type": self.event_type,
            "amount": self.amount,
            "occurred_at": self.occurred_at.isoformat(),
            "direction": self.direction,
            "is_external_cash_flow": self.is_external_cash_flow,
        }

        optional_fields = {
            "broker_event_id": self.broker_event_id,
            "raw_type": self.raw_type,
            "raw_subtype": self.raw_subtype,
            "currency": self.currency,
            "description": self.description,
        }
        for key, value in optional_fields.items():
            if value is not None:
                result[key] = value
        return result
