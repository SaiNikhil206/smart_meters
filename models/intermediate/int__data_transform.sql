with transform as(
    SELECT * from {{ ref('int__join_pmv')}}
),
override as (
    select * from {{ ref('stg__exception_override')}}
),
final as (
    select NVL(t.LATEST_VALID_CHSU_REPORT_KEY, t.LATEST_CHSU_REPORT_KEY)as CHSU_REPORT_KEY, CURRENT_TIMESTAMP() as LOAD_DT_TM,REGION, DATE_TRUNC('day',NVL(t.FIRST_VALID_CHSU_RECEIVED_DATE, cast(t.FIRST_CHSU_RECEIVED_DATE AS TIMESTAMP_TZ))) as PM_EVENT_KEY_DT, o.INCIDENT_NUMBER as INCIDENT_NUMBER from transform t join override o on t.commshub_key = o.commshub_key
)
select * from final