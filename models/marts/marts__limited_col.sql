WITH chsu_data as (
    SELECT * FROM {{ ref('int__join_pmv')}}
),
final as(
    select * from chsu_data LIMIT 49
)
select * from final