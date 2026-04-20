from gametracker.schema_org import extract_product


LDJSON_PRODUCT = """
<html><head>
<script type="application/ld+json">
{
  "@context": "https://schema.org/",
  "@type": "Product",
  "name": "Resident evil 9 Requiem PS5",
  "sku": "VGP50001272N",
  "brand": {"@type":"Brand","name":"Capcom"},
  "offers": {
    "@type":"Offer",
    "price":"369.90",
    "priceCurrency":"RON",
    "availability":"https://schema.org/InStock"
  }
}
</script>
</head></html>
"""


MICRODATA_PRODUCT = """
<html><body>
<div itemprop="offers" itemscope itemtype="http://schema.org/Offer">
<meta itemprop="priceCurrency" content="RON">
<meta itemprop="price" content="99.99">
<meta itemprop="availability" content="http://schema.org/InStock">
</div>
<h1><span itemprop="name">Joc PS5 EA SPORTS FC 25</span></h1>
<span itemprop="sku"> 237375 </span>
</body></html>
"""


def test_ldjson_basic():
    p = extract_product(LDJSON_PRODUCT)
    assert p is not None
    assert p.name == "Resident evil 9 Requiem PS5"
    assert p.price == 369.90
    assert p.currency == "RON"
    assert p.availability == "in_stock"
    assert p.sku == "VGP50001272N"
    assert p.brand == "Capcom"


def test_microdata_basic():
    p = extract_product(MICRODATA_PRODUCT)
    assert p is not None
    assert p.price == 99.99
    assert p.currency == "RON"
    assert p.availability == "in_stock"
    assert p.name == "Joc PS5 EA SPORTS FC 25"
    assert p.sku == "237375"


def test_no_product_returns_none():
    assert extract_product("<html><body>no schema here</body></html>") is None


def test_availability_mapping():
    # OutOfStock
    html = LDJSON_PRODUCT.replace("InStock", "OutOfStock")
    p = extract_product(html)
    assert p.availability == "out_of_stock"

    # PreOrder
    html = LDJSON_PRODUCT.replace("InStock", "PreOrder")
    p = extract_product(html)
    assert p.availability == "preorder"


def test_offers_list_takes_first():
    html = """<script type="application/ld+json">
    {"@type":"Product","name":"X","offers":[
      {"@type":"Offer","price":"10.00","priceCurrency":"RON","availability":"https://schema.org/InStock"},
      {"@type":"Offer","price":"20.00","priceCurrency":"RON"}
    ]}</script>"""
    p = extract_product(html)
    assert p.price == 10.00
