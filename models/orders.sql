with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

final as (

    select
        orders.order_id as order_id,
        orders.customer_id as customer_id,
        SUM(payments.amount) as amount


    from 
        orders
        left join payments using (order_id)
    
    where
        payments.status = 'success'
    
    group by
        order_id,
        customer_id

)

select * from final