from gametracker.matcher import (
    Candidate,
    detect_editions,
    detect_platform_ps5,
    match,
)


def c(title, price=0.0, url="", used=False):
    return Candidate(title=title, price_ron=price, url=url, is_used=used)


def test_ps5_detection():
    assert detect_platform_ps5("joc ps5 ea sports fc 25")
    assert detect_platform_ps5("resident evil requiem pentru playstation 5")
    assert not detect_platform_ps5("joc ps4 ea sports fc 25")
    assert not detect_platform_ps5("joc xbox series x")
    assert not detect_platform_ps5("nintendo switch 2")
    # 'ps500' should not match — token-bounded
    assert not detect_platform_ps5("ps500 fake")


def test_edition_detection():
    assert detect_editions("deluxe steelbook edition") == {"deluxe steelbook", "deluxe", "steelbook"}
    assert detect_editions("joc standard ps5") == {"standard"}
    assert detect_editions("no edition here") == set()


def test_picks_cheapest_when_no_edition():
    cands = [
        c("Resident Evil Requiem Deluxe Steelbook PS5", 580),
        c("Resident Evil Requiem PS5", 369),
        c("Resident Evil Requiem Lenticular PS5", 432),
    ]
    r = match("resident evil requiem", cands)
    assert r.winner is not None
    assert r.winners
    assert r.winner.price_ron == 369
    assert "Deluxe" not in r.winner.title


def test_edition_in_query_filters():
    cands = [
        c("Resident Evil Requiem Deluxe Steelbook PS5", 580),
        c("Resident Evil Requiem PS5", 369),
    ]
    r = match("resident evil requiem deluxe steelbook", cands)
    assert r.winner is not None
    assert r.winner.price_ron == 580


def test_ps5_filter_excludes_other_platforms():
    cands = [
        c("Joc Xbox Series Resident Evil Requiem", 300),
        c("Joc Nintendo Switch 2 Resident Evil Requiem", 255),
        c("Joc PC Resident Evil Requiem", 350),
        c("Joc PS5 Resident Evil Requiem", 369),
    ]
    r = match("resident evil requiem", cands)
    assert r.winner is not None
    assert r.winners
    assert "PS5" in r.winner.title


def test_below_min_score_rejected():
    cands = [c("FIFA 25 PS5", 200)]
    r = match("resident evil requiem", cands)
    assert r.winner is None


def test_empty_candidates():
    r = match("anything", [])
    assert r.winner is None
    assert r.winners == []


def test_returns_both_new_and_sh_when_present():
    cands = [
        c("Joc PS5 Silent Hill 2 Remake", 199, used=False),
        c("Joc PS5 Silent Hill 2 Remake Second-Hand SH", 149, used=True),
    ]
    r = match("silent hill 2 remake", cands)
    assert len(r.winners) == 2
    used = [w for w in r.winners if w.is_used]
    new = [w for w in r.winners if not w.is_used]
    assert len(used) == 1 and used[0].price_ron == 149
    assert len(new) == 1 and new[0].price_ron == 199
    # backward-compat single winner = cheapest
    assert r.winner.price_ron == 149


def test_used_keyword_in_query():
    cands = [
        c("Joc PS5 Resident Evil Requiem", 369, used=False),
        c("Joc PS5 Resident Evil Requiem Second-Hand SH", 250, used=True),
    ]
    r = match("resident evil requiem sh", cands)
    assert r.winner is not None
    assert r.winner.is_used is True


def test_base_edition_preferred_on_ties():
    cands = [
        c("Resident Evil Requiem Deluxe PS5", 369),
        c("Resident Evil Requiem PS5", 369),
    ]
    r = match("resident evil requiem", cands)
    assert r.winner is not None
    assert r.winners
    assert "Deluxe" not in r.winner.title
