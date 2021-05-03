from collections import namedtuple

OrderStatusObject = namedtuple(
    "OrderStatus",
    [
        "new_order",
        "canceled_order",
        "partially_filled_order",
        "filled_order",
        "unprocessed_order",
    ],
)

OrderStatus = OrderStatusObject(
    new_order="new",
    canceled_order="canceled",
    filled_order="filled",
    partially_filled_order="partially_fill",
    unprocessed_order="unprocessed",
)
