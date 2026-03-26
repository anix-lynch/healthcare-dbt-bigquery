-- mart_condition_kpis: clinical KPIs per medical condition
-- Used for: operational dashboards, readmission analysis, billing benchmarks

with patients as (
    select * from {{ ref('stg_patients') }}
),

condition_stats as (
    select
        medical_condition,
        count(*)                                            as total_patients,
        avg(billing_amount)                                 as avg_billing_usd,
        sum(billing_amount)                                 as total_billed_usd,
        avg(length_of_stay_days)                            as avg_los_days,
        max(length_of_stay_days)                            as max_los_days,
        countif(admission_type = 'Emergency') / count(*)    as emergency_rate,
        countif(test_results = 'Abnormal') / count(*)       as abnormal_test_rate,
        countif(test_results = 'Normal') / count(*)         as normal_test_rate,
        count(distinct doctor)                              as unique_doctors,
        count(distinct hospital)                            as unique_hospitals,
        min(date_of_admission)                              as first_admission_date,
        max(date_of_admission)                              as last_admission_date

    from patients
    group by medical_condition
)

select
    medical_condition,
    total_patients,
    round(avg_billing_usd, 2)                   as avg_billing_usd,
    round(total_billed_usd, 2)                  as total_billed_usd,
    round(avg_los_days, 1)                       as avg_los_days,
    max_los_days,
    round(emergency_rate * 100, 1)              as emergency_pct,
    round(abnormal_test_rate * 100, 1)          as abnormal_test_pct,
    round(normal_test_rate * 100, 1)            as normal_test_pct,
    unique_doctors,
    unique_hospitals,
    first_admission_date,
    last_admission_date

from condition_stats
order by total_patients desc
