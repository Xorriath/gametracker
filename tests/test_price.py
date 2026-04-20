import pytest

from gametracker.price import PriceParseError, parse_price


def test_english_decimal():
    assert parse_price("99.99") == 99.99
    assert parse_price("369.00") == 369.00
    assert parse_price(412.22) == 412.22


def test_romanian_comma_no_space():
    assert parse_price("369,90Lei") == 369.90


def test_romanian_comma_with_space():
    assert parse_price("255,00 Lei") == 255.00
    assert parse_price("255 lei") == 255.00


def test_emag_split_markup():
    s = '255<sup><small class="mf-decimal">&#44;</small>00</sup> <span>Lei</span>'
    assert parse_price(s) == 255.00


def test_thousands_with_comma_decimal():
    assert parse_price("1.234,56 lei") == 1234.56


def test_integer_only():
    assert parse_price("500") == 500.0
    assert parse_price("500 RON") == 500.0


def test_ron_suffix():
    assert parse_price("412.22 RON") == 412.22


def test_empty_and_none():
    with pytest.raises(PriceParseError):
        parse_price(None)
    with pytest.raises(PriceParseError):
        parse_price("")
    with pytest.raises(PriceParseError):
        parse_price("   ")
    with pytest.raises(PriceParseError):
        parse_price("no digits here")


def test_negative_rejected():
    with pytest.raises(PriceParseError):
        parse_price("-10")


def test_passthrough_numeric():
    assert parse_price(0) == 0.0
    assert parse_price(369) == 369.0
