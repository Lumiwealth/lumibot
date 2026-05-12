from decimal import Decimal
from typing import Any


def check_numeric(
    value: Any,
    expected_type: type[Any],
    error_message: str,
    positive: bool = True,
    strict: bool = False,
    nullable: bool = False,
    ratio: bool = False,
    allow_negative: bool = True,
) -> Any:
    if nullable and value is None:
        return None

    error = ValueError(error_message)

    if isinstance(value, str) or (expected_type == Decimal and not isinstance(value, Decimal)):
        try:
            value = expected_type(value)
        except Exception:
            raise error from None

    if not allow_negative:
        if positive:
            if strict:
                if value <= 0:
                    raise error
            else:
                if value < 0:
                    raise error

    if ratio:
        if value >= 0:
            if value > 1:
                raise error
        else:
            if value < -1:
                raise error

    return value


def check_positive(value: Any, expected_type: type[Any], custom_message: str = "", strict: bool = False) -> Any:
    if strict:
        error_message = f"{value!r} is not a strictly positive value."
    else:
        error_message = f"{value!r} is not a positive value."
    if custom_message:
        error_message = f"{error_message} {custom_message}"

    result = check_numeric(
        value,
        expected_type,
        error_message,
        strict=strict,
    )
    return result


def check_quantity(quantity: Any, custom_message: str = "") -> Decimal:
    error_message = f"{quantity!r} is not a positive Decimal."
    if custom_message:
        error_message = f"{error_message} {custom_message}"

    quantity = Decimal(quantity)
    result = check_numeric(
        quantity,
        Decimal,
        error_message,
        strict=True,
    )
    return result


def check_price(price: Any, custom_message: str = "", nullable: bool = True, allow_negative: bool = True) -> Any:
    error_message = f"{price!r} is not a valid price."
    if custom_message:
        error_message = f"{error_message} {custom_message}"

    result = check_numeric(price, float, error_message, strict=True, nullable=nullable, allow_negative=allow_negative)
    return result
