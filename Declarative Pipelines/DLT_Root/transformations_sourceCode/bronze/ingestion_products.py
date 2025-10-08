import dlt

products_rules={
    "rule_1" :"product_id is NOT NULL",
    "rule_2" :"price >=0"
}

# Ingesting Products
@dlt.table(
    name='products_stg'
)
@dlt.expect_all(products_rules)

def products_stg():
    df=spark.readStream.table("dltsim.source.products")
    return df
