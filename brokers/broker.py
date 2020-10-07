class Broker:

    def __init__(self):
        self.new_orders = set()
        self.partially_filled_orders = set()
        self.filled_orders = set()
        self.canceled_orders = set()

    #==== Global helper methods=====================

    @staticmethod
    def get_chunks(l, chunk_size):
        chunks = []
        for i in range(0, len(l), chunk_size):
            chunks.append(l[i:i + chunk_size])
        return chunks
