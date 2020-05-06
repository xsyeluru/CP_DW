/***
  DML to Load LMSI_CHANNEL table

  Sources: 
  Target: 
  Logic: 
  Table Type: 
  Parameters:  
  
  Modification Log:
  ---------------------------------------------------------------------------------------------------
  Date                      Author                  Description
  ---------------------------------------------------------------------------------------------------
  04/16/2020                Varun George           
  ----------------------------------------------------------------------------------------------------
 ***/
/*LMSI_CHANNEL Table Incremental Approach*/

/*STG_INT_LMSI_CHANNEL Table*/

/*COPY Command to load the Source Staging Layer*/
Truncate Table {db}.{landing_schema}.STG_INT_LMSI_CHANNEL;

COPY INTO {db}.{landing_schema}.STG_INT_LMSI_CHANNEL
(
  REGION
, CHNL_OF_DIST
, CH_OF_DIST_DESC
, TERR_RPT_GRP
, SEARCH_DISPLAY
, ACTIVE
, FILE_NAME
, ROW_NUMBER
, CREATED_TIMESTAMP
, ELT_BATCH_ID
)
FROM 
	(
	Select $1, $2, $3, $4, $5, $6, METADATA$FILENAME ,METADATA$FILE_ROW_NUMBER,CURRENT_TIMESTAMP::TIMESTAMP_NTZ,%(ELT_BATCH_ID)s
	FROM '@{db}.{landing_schema}.STAGE_S3_LMSI_DAY/%(YYYYMMDD)s/'
	)
PATTERN = '.*Channel.*'--case sensitive
FILE_FORMAT ={db}.{landing_schema}.FF_CSV_TAB_SKIPHEAD_1 
encryption = (TYPE = 'AWS_SSE_S3' ) 
ON_ERROR = abort_statement ;


/*Load the Target Staging Layer*/
Truncate Table {db}.{curated_schema}.STG_LMSI_CHANNEL;

Insert Into {db}.{curated_schema}.STG_LMSI_CHANNEL
(
  CHANNEL_OF_DISTRIBUTION_CODE
, TERRITORY_REPORTING_GROUP
, CHANNEL_OF_DISTRIBUTION_DESCRIPTION
, REGION
, SEARCH_DISPLAY
, ISACTIVE
, CREATED_TIMESTAMP
, UPDATED_TIMESTAMP
, ERROR_FLG
, ERROR_TYPE
, ERROR_MESSAGE
, ELT_BATCH_ID
)
Select 
  nvl(CHNL_OF_DIST,'N/A') As CHANNEL_OF_DISTRIBUTION_CODE
, TERR_RPT_GRP As TERRITORY_REPORTING_GROUP
, CH_OF_DIST_DESC As CHANNEL_OF_DISTRIBUTION_DESCRIPTION
, REGION As REGION
, SEARCH_DISPLAY As SEARCH_DISPLAY
, ACTIVE As ISACTIVE
, Created_Timestamp::timestamp_ntz
,CURRENT_TIMESTAMP::TIMESTAMP_NTZ As Updated_Timestamp
, 'N' As ERROR_FLG
, NULL As ERROR_MESSAGE
, NULL As ERROR_TYPE
, %(ELT_BATCH_ID)s As ELT_BATCH_ID
From 
{db}.{landing_schema}.STG_INT_LMSI_CHANNEL
;

/*CHANNEL_OF_DISTRIBUTION_CODE Is Null*/
Update {db}.{curated_schema}.STG_LMSI_CHANNEL STG 
Set ERROR_FLG='Y',ERROR_MESSAGE='CHANNEL_OF_DISTRIBUTION_CODE which is a PK is having NULL values', ERROR_TYPE='ERROR'
Where CHANNEL_OF_DISTRIBUTION_CODE Is NULL;

/*Duplicates error Validation*/
Update {db}.{curated_schema}.STG_LMSI_CHANNEL STG 
Set ERROR_FLG='Y',ERROR_MESSAGE='Duplicate record on Key Columns CHANNEL_OF_DISTRIBUTION_CODE, TERRITORY_REPORTING_GROUP',ERROR_TYPE='ERROR'
From 
(
Select CHNL_OF_DIST,TERR_RPT_GRP From {db}.{landing_schema}.STG_INT_LMSI_CHANNEL 
Qualify row_number() over (partition by CHNL_OF_DIST,TERR_RPT_GRP order by Created_Timestamp::TIMESTAMP_NTZ DESC) >1
) TGT
Where STG.CHANNEL_OF_DISTRIBUTION_CODE=TGT.CHNL_OF_DIST
And STG.TERRITORY_REPORTING_GROUP=TGT.TERR_RPT_GRP
And STG.ERROR_FLG='N'
;

/*Load the Target Table*/
--TRUNCATE TABLE {db}.{main_schema}.LMSI_CHANNEL;

DELETE from {db}.{main_schema}.LMSI_CHANNEL TGT where exists
(select 1 from {db}.{curated_schema}.STG_LMSI_CHANNEL STG where TGT.CHANNEL_OF_DISTRIBUTION_CODE=STG.CHANNEL_OF_DISTRIBUTION_CODE and 
TGT.TERRITORY_REPORTING_GROUP=STG.TERRITORY_REPORTING_GROUP and ERROR_FLG='N');

SCRIPT = [{"script": "DML/LMSI_CHANNEL_dml.sql","execution_order":"1"}]

Insert Into {db}.{main_schema}.LMSI_CHANNEL
(
  CHANNEL_OF_DISTRIBUTION_CODE
, TERRITORY_REPORTING_GROUP
, CHANNEL_OF_DISTRIBUTION_DESCRIPTION
, REGION
, SEARCH_DISPLAY
, ISACTIVE
, CREATED_TIMESTAMP
, UPDATED_TIMESTAMP
, ELT_BATCH_ID
)
SELECT
  CHANNEL_OF_DISTRIBUTION_CODE As CHANNEL_OF_DISTRIBUTION_CODE
, TERRITORY_REPORTING_GROUP As TERRITORY_REPORTING_GROUP
, CHANNEL_OF_DISTRIBUTION_DESCRIPTION As CHANNEL_OF_DISTRIBUTION_DESCRIPTION
, REGION As REGION
, SEARCH_DISPLAY As SEARCH_DISPLAY
, ISACTIVE As ISACTIVE
, Created_Timestamp::TIMESTAMP_NTZ
, Updated_Timestamp::TIMESTAMP_NTZ
, %(ELT_BATCH_ID)s As ELT_BATCH_ID
From
{db}.{curated_schema}.STG_LMSI_CHANNEL
Where ERROR_FLG='N'
;