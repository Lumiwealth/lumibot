"""Process-wide switch: record when an order's status changed (live only).

Kept in its own tiny module so a live broker can turn it on without importing
``lumibot.entities.order`` (scheduled runs defer that import until an order exists).
"""

ENABLED = False
