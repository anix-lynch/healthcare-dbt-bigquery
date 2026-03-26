-- mart_monthly_admissions: time-series admission and revenue trends
-- Used for: capacity planning, seasonal analysis, revenue forecasting

with patients as (
    select * from {{ ref('stg_patients') }}
),

monthly as (
    select
        date_trunc(date_of_admission, month)            as admission_month,
        admission_type,
        count(*)                                        as admissions,
        sum(billing_amount)                             as total_billed_usd,
        avg(billing_amount)                             as avg_billing_usd,
        avg(length_of_stay_days)                        as avg_los_days,
        countif(test_results = 'Abnormal')              as abnormal_tests,
        count(distinct medical_condition)               as conditions_seen

    from patients
    group by admission_month, admission_type
)

select
    admission_month,
    admission_type,
    admissions,
    round(total_billed_usd, 2)          as total_billed_usd,
    round(avg_billing_usd, 2)           as avg_billing_usd,
    round(avg_los_days, 1)              as avg_los_days,
    abnormal_tests,
    conditions_seen,
    sum(admissions) over (
        partition by admission_type
        order by admission_month
        rows between unbounded preceding and current row
    )                                   as cumulative_admissions

from monthly
order by admission_month, admission_type
