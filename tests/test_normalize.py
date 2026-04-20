from gametracker.normalize import normalize_query


def test_lowercase_and_strip_punct():
    assert normalize_query("Resident Evil: Requiem!") == "resident evil requiem"


def test_collapse_whitespace():
    assert normalize_query("  Hogwarts   Legacy  ") == "hogwarts legacy"


def test_accents_stripped():
    assert normalize_query("Pokémon Scărlet") == "pokemon scarlet"


def test_digits_kept():
    assert normalize_query("FIFA 25") == "fifa 25"


def test_empty():
    assert normalize_query("   ") == ""
    assert normalize_query("") == ""
