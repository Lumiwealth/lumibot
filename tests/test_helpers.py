from lumibot.tools.helpers import has_more_than_n_decimal_places


def test_has_more_than_n_decimal_places():
    assert has_more_than_n_decimal_places(1.2, 0) == True
    assert has_more_than_n_decimal_places(1.2, 1) == False
    assert has_more_than_n_decimal_places(1.22, 0) == True
    assert has_more_than_n_decimal_places(1.22, 1) == True
    assert has_more_than_n_decimal_places(1.22, 5) == False

    assert has_more_than_n_decimal_places(1.2345, 0) == True
    assert has_more_than_n_decimal_places(1.2345, 1) == True
    assert has_more_than_n_decimal_places(1.2345, 3) == True
    assert has_more_than_n_decimal_places(1.2345, 4) == False
    assert has_more_than_n_decimal_places(1.2345, 5) == False


