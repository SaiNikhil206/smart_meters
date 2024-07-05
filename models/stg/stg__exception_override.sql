with exception as (
    select * from {{source('smart_meters_data','pmv_exception_override')}}
)
select * from exception