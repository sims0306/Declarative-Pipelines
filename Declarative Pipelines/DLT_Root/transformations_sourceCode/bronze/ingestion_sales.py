import dlt

#Sales expectations
sales_rules={
  'rule_1':'sales_id IS NOT NULL'
}

# creating empty streaming table
dlt.create_streaming_table(
  name='sales_stg',
  expect_all_or_drop=sales_rules
)

#Creating East sales flow
@dlt.append_flow(target='sales_stg')
def east_sales():
    df=spark.readStream.table('dltsim.source.sales_east')
    return df


#Creating West sales flow
@dlt.append_flow(target='sales_stg')
def west_sales():
    df=spark.readStream.table('dltsim.source.sales_west')
    return df