with new_chsu as (
    select * from {{ ref('stg__dim_chsu')}}
),
joined_chsu as (
    select * from {{ ref('int__join_pmv')}}
),
final as (
    select jc.* from new_chsu nc left join joined_chsu jc on jc.LATEST_VALID_CHSU_REPORT_KEY = nc.chsu_key where jc.chsu_key is not NULL
)
select * from final