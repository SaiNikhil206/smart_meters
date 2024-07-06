{% set report_start_date = "DATEADD(MONTH, -1, CURRENT_TIMESTAMP())" %}
{% set report_end_date = "CURRENT_TIMESTAMP()" %}



with commshub as (
    select * from {{ ref('stg__dim_commshub')}}
),
chsu as (
    select * from {{ ref('stg__dim_chsu')}}
),
joined as (
    select *,dateadd(second, uniform(0, datediff(second, '2024-05-01 00:00:00', '2024-07-31 23:59:59'), random()), timestamp '2024-05-01 00:00:00') as FIRST_CHSU_RECEIVED_DATE,  from commshub c LEFT JOIN CHSU ch on coalesce(c.FIRST_VALID_CHSU_REPORT_KEY,c.FIRST_CHSU_REPORT_KEY) = ch.CHSU_KEY where ch.chsu_key is NOT NULL
),
revised as (
    select COMMSHUB_KEY,
        dateadd(second, uniform(0, datediff(second, '2024-06-01 00:00:00', '2024-06-30 23:59:59'), random()), timestamp '2024-06-01 00:00:00') as INSTALLED_DATE,
        FIRST_CHSU_RECEIVED_DATE as FIRST_VALID_CHSU_RECEIVED_DATE,
        FIRST_CONNECTED_DATE,
        REGION,
        TEST_HUB,
        NO_OF_CHSUS,
        NO_OF_VALID_CHSUS,
        LATEST_CHSU_REPORT_KEY,
        coalesce(LATEST_VALID_CHSU_REPORT_KEY,LATEST_CHSU_REPORT_KEY) as LATEST_VALID_CHSU_REPORT_KEY,
        coalesce(FIRST_VALID_CHSU_REPORT_KEY,FIRST_CHSU_REPORT_KEY) as FIRST_VALID_CHSU_REPORT_KEY,
        FIRST_CHSU_REPORT_KEY,
        CHSU_KEY,
        JOB_TYPE,
        FIRST_CHSU_RECEIVED_DATE 
        from joined
),
final as(
    select COMMSHUB_KEY,
        INSTALLED_DATE,
        FIRST_VALID_CHSU_RECEIVED_DATE,
        INSTALLED_DATE as FIRST_CONNECTED_DATE,
        REGION,
        TEST_HUB,
        NO_OF_CHSUS,
        NO_OF_VALID_CHSUS,
        LATEST_CHSU_REPORT_KEY,
        coalesce(LATEST_VALID_CHSU_REPORT_KEY,LATEST_CHSU_REPORT_KEY) as LATEST_VALID_CHSU_REPORT_KEY,
        coalesce(FIRST_VALID_CHSU_REPORT_KEY,FIRST_CHSU_REPORT_KEY) as FIRST_VALID_CHSU_REPORT_KEY,
        FIRST_CHSU_REPORT_KEY,
        CHSU_KEY,
        JOB_TYPE,
        FIRST_CHSU_RECEIVED_DATE from revised where FIRST_CHSU_RECEIVED_DATE BETWEEN {{ report_start_date }} AND {{ report_end_date }}
)
select * from final