with exclusions as (
    select * from {{ ref('int__join_pmv')}}
),
final as (
    select 	COMMSHUB_KEY,
	FIRST_CONNECTED_DATE,
	FIRST_VALID_CHSU_RECEIVED_DATE ,
	REGION,
	TEST_HUB,
	NO_OF_CHSUS,
	NO_OF_VALID_CHSUS,
	LATEST_CHSU_REPORT_KEY,
	LATEST_VALID_CHSU_REPORT_KEY,
	FIRST_VALID_CHSU_REPORT_KEY,
	FIRST_CHSU_REPORT_KEY,
	CHSU_KEY,
	JOB_TYPE,
	INSTALLED_DATE,
	REPORT_START_DATE,
	REPORT_END_DATE,
	FIRST_CHSU_RECEIVED_DATE,
    case 
        when TEST_HUB = 'Y' THEN 'TEST HUB'
        when REGION IS NULL THEN 'NO REGION POPULATED'
        when JOB_TYPE = 'Replacement CHF Install' THEN 'REPLACEMENT CHF INSTALL'
        when NO_OF_CHSUS > 0 and NO_OF_VALID_CHSUS = 0 THEN 'CHSU INVALID'
        else 'OTHER'
    end as exclusions
    from exclusions
)
select * from final