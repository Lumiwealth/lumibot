class Broker:

    def __init__(self):
        self.new_orders = []
        self.partially_filled_orders = []
        self.filled_orders = []
        self.canceled_orders = []
