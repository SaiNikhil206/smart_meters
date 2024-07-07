with pm_f as (
    select * from {{ ref('marts__pm_f')}}
),
commshub as (
    select * from {{ ref('stg__dim_commshub')}}
),
exception as (
    select * from {{ ref('stg__dim_adhoc_exception')}} 
),
adhoc_hub as (
    select c.COMMSHUB_KEY, e.exception_code as adhoc_hub_exc_code from commshub c join exception e on c.commshub_key=e.commshub_key where (affected_pm = 'PM 1.1' or affected_pm = 'All Schedule 2.2') and (FIRST_VALID_CHSU_RECEIVED_DATE between e.effective_from_dt_tm and e.effective_to_dt_tm) and DELETE_FLAG = 'N'
),
bau_hub as (
     select c.COMMSHUB_KEY,e.exception_code as bau_hub_exc_code from commshub c join exception e on c.commshub_key=e.commshub_key where (affected_pm = 'PM 1.1' or affected_pm = 'All Schedule 2.2') and (FIRST_VALID_CHSU_RECEIVED_DATE between e.effective_from_dt_tm and e.effective_to_dt_tm)
),
joined as (
    SELECT 
    pm_f.commshub_key,
     CASE
            WHEN adhoc_hub_exc_code IS NOT NULL THEN adhoc_hub_exc_code
            WHEN bau_hub_exc_code IS NOT NULL THEN bau_hub_exc_code
            ELSE NULL
        END AS exception_code,
    FROM 
        pm_f
    LEFT JOIN 
        adhoc_hub ah 
        ON pm_f.commshub_key = ah.commshub_key
    LEFT JOIN 
        bau_hub bh
        ON pm_f.commshub_key = bh.commshub_key
),
final as (
    SELECT
        *,
        CASE
            WHEN exception_code IS NOT NULL THEN 'E'
            ELSE 'F'
        END AS PM_FLAG
    FROM
        joined
)
select * from final
