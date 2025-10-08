import dlt
# customer expectations
customer_rules={
    "rule_1" :"customer_id is NOT NULL",
    "rule_2" :"customer_name is NOT NULL"
}
#Ingesting customer
@dlt.table(
    name='customer_stg'
)
@dlt.expect_all_or_drop(customer_rules)
def customer_stg():
    df=spark.readStream.table('dltsim.source.customers')
    return df