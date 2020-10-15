from services import AlpacaService


class AlpacaData(AlpacaService):
    def __init__(self, config, max_workers=20, chunk_size=100):
        # Calling AlpacaService constructor
        AlpacaService.__init__(
            self, config, max_workers=max_workers, chunk_size=chunk_size
        )
