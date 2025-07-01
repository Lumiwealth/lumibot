import warnings
from enum import Enum
from decimal import Decimal

# Custom string enum implementation for Python 3.9 compatibility
class StrEnum(str, Enum):
    """
    A string enum implementation that works with Python 3.9+
    
    This class extends str and Enum to create string enums that:
    1. Can be used like strings (string methods, comparison)
    2. Are hashable (for use in dictionaries, sets, etc.)
    3. Can be used in string comparisons without explicit conversion
    """
    def __str__(self):
        return self.value
        
    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        return super().__eq__(other)
    
    def __hash__(self):
        # Use the hash of the enum member, not the string value
        # This ensures proper hashability while maintaining enum identity
        return super().__hash__()

def check_numeric(
    input, type, error_message, positive=True, strict=False, nullable=False, ratio=False, allow_negative=True
):
    if nullable and input is None:
        return None

    error = ValueError(error_message)

    if isinstance(input, str) or (type == Decimal and not isinstance(input, Decimal)):
        try:
            input = type(input)
        except:
            raise error

    if not allow_negative:
        if positive:
            if strict:
                if input <= 0:
                    raise error
            else:
                if input < 0:
                    raise error

    if ratio:
        if input >= 0:
            if input > 1:
                raise error
        else:
            if input < -1:
                raise error

    return input


def check_positive(input, type, custom_message="", strict=False):
    if strict:
        error_message = "%r is not a strictly positive value." % input
    else:
        error_message = "%r is not a positive value." % input
    if custom_message:
        error_message = f"{error_message} {custom_message}"

    result = check_numeric(
        input,
        type,
        error_message,
        strict=strict,
    )
    return result

def check_quantity(quantity, custom_message=""):
    error_message = "%r is not a positive Decimal." % quantity
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


def check_price(price, custom_message="", nullable=True, allow_negative=True):
    error_message = "%r is not a valid price." % price
    if custom_message:
        error_message = f"{error_message} {custom_message}"

    result = check_numeric(price, float, error_message, strict=True, nullable=nullable, allow_negative=allow_negative)
    return result
