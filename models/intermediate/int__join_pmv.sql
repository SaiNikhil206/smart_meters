with commshub as (
    select * from {{ ref('stg__dim_commshub')}}
),
chsu as (
    select * from {{ ref('stg__dim_chsu')}}
),
joined as (
    select * from commshub c LEFT JOIN CHSU ch on coalesce(c.FIRST_VALID_CHSU_REPORT_KEY,c.FIRST_CHSU_REPORT_KEY)= ch.CHSU_KEY where ch.chsu_key is NOT NULL and FIRST_VALID_CHSU_RECEIVED_DATE is NOT NULL and FIRST_CONNECTED_DATE IS NOT NULL
),
final as (
    SELECT *,DATEADD(MONTH, -1, CURRENT_TIMESTAMP()) AS report_start_date, CURRENT_TIMESTAMP() as report_end_date,dateadd(second, uniform(0, datediff(second, '2024-05-01 00:00:00', '2024-07-31 23:59:59'), random()), timestamp '2024-05-01 00:00:00') as FIRST_CHSU_RECEIVED_DATE FROM joined
    where FIRST_CHSU_RECEIVED_DATE between report_start_date and report_end_date
)
select * from final
