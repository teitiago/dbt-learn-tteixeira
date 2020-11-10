with customers as (

    select * from {{ ref('stg_customers') }}

),

stg_orders as (

    select * from {{ ref('stg_orders') }}

),

orders as (

    select * from {{ ref('orders') }}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        SUM(orders.amount) as lifetime_value

    from
    stg_orders
    left join orders using (customer_id)

    group by 1

),

total_amount as (

    select
        customers.customer_id,
        SUM(orders.amount) as lifetime_value
    from
        customers
        left join orders using (customer_id)

    group by
        customer_id
),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        customer_orders.lifetime_value as lifetime_value

    from
        customers
        left join customer_orders using (customer_id)
        -- left join total_amount using (customer_id)


)

select * from final