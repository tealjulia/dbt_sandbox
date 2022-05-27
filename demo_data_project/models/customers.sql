{{ config(materialized='table') }}

with accounts as (
    select * from {{ ref('stg_accounts') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

plans as (
    select * from {{ ref('stg_plans') }}
),

account_payments as (
    select account_id,
    ROUND(sum( amount )::numeric,0) as total_payment 
        from payments
        
    where payments.state='successful'

    group by account_id
),

account_failed_payments as (
    select account_id,
    count( payment_id ) as failed_count
        from payments
    where payments.state='failed'

    group by account_id
),

final as (

    select  
        accounts.account_id,
        accounts.account_name,
        account_payments.total_payment as account_lifetime_value,
        account_failed_payments.failed_count as account_failed_payments

    from accounts

    left join account_payments 
        on accounts.account_id = account_payments.account_id

    left join account_failed_payments   
        on accounts.account_id = account_failed_payments.account_id
)

select * from final