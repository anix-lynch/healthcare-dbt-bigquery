-- mart_billing_by_insurer: revenue cycle analysis by insurance provider
-- Used for: RCM teams, billing benchmarking, payer mix analysis

with patients as (
    select * from {{ ref('stg_patients') }}
),

billing as (
    select
        insurance_provider,
        count(*)                                        as total_claims,
        sum(billing_amount)                             as total_billed_usd,
        avg(billing_amount)                             as avg_claim_usd,
        min(billing_amount)                             as min_claim_usd,
        max(billing_amount)                             as max_claim_usd,
        avg(length_of_stay_days)                        as avg_los_days,
        countif(test_results = 'Abnormal') / count(*)   as abnormal_rate,
        approx_quantiles(billing_amount, 2)[offset(1)]  as median_claim_usd

    from patients
    group by insurance_provider
)

select
    insurance_provider,
    total_claims,
    round(total_billed_usd, 2)          as total_billed_usd,
    round(avg_claim_usd, 2)             as avg_claim_usd,
    round(min_claim_usd, 2)             as min_claim_usd,
    round(max_claim_usd, 2)             as max_claim_usd,
    round(median_claim_usd, 2)          as median_claim_usd,
    round(avg_los_days, 1)              as avg_los_days,
    round(abnormal_rate * 100, 1)       as abnormal_test_pct

from billing
order by total_billed_usd desc
