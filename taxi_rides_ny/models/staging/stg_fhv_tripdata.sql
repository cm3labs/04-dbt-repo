{{
    config(
        materialized='view'
    )
}}

with fhv_data_partitoned_clustered as 
(
  select * from {{ source('staging','fhv_data_partitoned_clustered') }}
),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
        {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("integer")) }} as dispatching_base_num,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pulocationid,
        {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dolocationid,
        {{ dbt.safe_cast("sr_flag", api.Column.translate_type("integer")) }} as sr_flag,
        {{ dbt.safe_cast("affiliated_base_number", api.Column.translate_type("integer")) }} as affiliated_base_number

    from fhv_data_partitoned_clustered
    where EXTRACT(YEAR FROM DATE(pickup_datetime)) = 2019

)

select * from renamed

-- dbt build --select stg_fhv_tripdata --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}