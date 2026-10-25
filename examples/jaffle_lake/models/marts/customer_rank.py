def model(dbt, spark):
    dbt.config(materialized='table', tags=['gold'])

    customers = spark.read.table(dbt.ref('customers'))
    orders = spark.read.table(dbt.ref('order_events'))

    return (orders.groupBy('customer_id')
            .count()
            .withColumnRenamed('count', 'order_count')
            .join(customers, 'customer_id')
            .orderBy('order_count', ascending=False))
