with transform as(
    SELECT * from {{ ref('int__exclusions')}}
),
override as (
    select * from {{ ref('stg__exception_override')}}
),
final as (
    select 
	distinct(t.COMMSHUB_KEY),
	FIRST_CONNECTED_DATE,
	FIRST_VALID_CHSU_RECEIVED_DATE,
	TEST_HUB,
	NO_OF_CHSUS,
	NO_OF_VALID_CHSUS,
	LATEST_CHSU_REPORT_KEY,
	LATEST_VALID_CHSU_REPORT_KEY,
	FIRST_VALID_CHSU_REPORT_KEY,
	FIRST_CHSU_REPORT_KEY,
	CHSU_KEY,
	LATEST_VALID_JOB_TYPE,
	INSTALLED_DATE,
	FIRST_CHSU_RECEIVED_DATE,
    NVL(t.LATEST_VALID_CHSU_REPORT_KEY, t.LATEST_CHSU_REPORT_KEY) as CHSU_REPORT_KEY, 
        CURRENT_TIMESTAMP() as LOAD_DT_TM,REGION, 
        DATE_TRUNC('day',NVL(t.FIRST_VALID_CHSU_RECEIVED_DATE, cast(t.FIRST_CHSU_RECEIVED_DATE AS TIMESTAMP_TZ))) as PM_EVENT_KEY_DT, 
        INCIDENT_NUMBER
        from transform t 
        join override o on t.commshub_key = o.commshub_key where o.INCIDENT_NUMBER is NOT NULL
)
select * from final