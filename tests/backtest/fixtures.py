import pytest
import datetime
from lumibot.backtesting import PolygonDataBacktesting  # Asegúrate de importar correctamente tu clase

@pytest.fixture
def polygon_data_backtesting():
    # Configura aquí las propiedades iniciales necesarias para tu objeto de prueba
    datetime_start = datetime.datetime(2023, 1, 1)
    datetime_end = datetime.datetime(2023, 2, 1)
    api_key = "fake_api_key"
    pandas_data = {}
    
    # Crea una instancia de PolygonDataBacktesting con los valores necesarios
    polygon_data_instance = PolygonDataBacktesting(
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        pandas_data=pandas_data,
        api_key=api_key,
        has_paid_subscription=False
    )
    
    return polygon_data_instance
