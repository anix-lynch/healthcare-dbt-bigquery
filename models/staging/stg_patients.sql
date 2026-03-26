-- stg_patients: clean + type-cast raw patient records
with source as (
    select * from {{ source('raw', 'raw_patients') }}
),

cleaned as (
    select
        -- identifiers
        name,
        room_number,

        -- demographics
        age,
        gender,
        blood_type,

        -- clinical
        medical_condition,
        medication,
        test_results,

        -- admission
        admission_type,
        date_of_admission,
        discharge_date,
        date_diff(discharge_date, date_of_admission, day) as length_of_stay_days,

        -- hospital
        doctor,
        hospital,

        -- financial
        insurance_provider,
        round(billing_amount, 2) as billing_amount

    from source
    where date_of_admission is not null
      and discharge_date is not null
      and billing_amount > 0
)

select * from cleaned
