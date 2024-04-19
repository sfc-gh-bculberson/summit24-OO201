CREATE OR REPLACE FUNCTION RESTORE_FILE(bucket string, file_name string, days int)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('boto3', 'snowflake-snowpark-python')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (aws_apis_access_integration) 
SECRETS = ('aws_iam_key' = my_aws_access_key_iam, 'aws_iam_secret' = my_aws_secret_key_iam)
AS
$$
import _snowflake
import boto3
import botocore

def run(bucket, file_name, days):
    k = _snowflake.get_generic_secret_string('aws_iam_key')
    s = _snowflake.get_generic_secret_string('aws_iam_secret')
    s3_client = boto3.client('s3', aws_access_key_id=k, aws_secret_access_key=s)
    response = s3_client.head_object(Bucket=bucket, Key=file_name)
    if "false" in response['Restore']:
        return 'Restored'
    try:
        response = s3_client.restore_object(Bucket=bucket, Key=file_name, RestoreRequest={'Days': days, 'GlacierJobParameters': {'Tier': 'Standard'}})
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'RestoreAlreadyInProgress':
            return 'RestoreAlreadyInProgress'
        else:
            raise e
    return 'RestoreInProgress'
$$;

CREATE OR REPLACE PROCEDURE RESTORE_EXAMPLE_DAYS(SDATE DATE, EDATE DATE, DAYS NUMBER)
RETURNS INT
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    TOTALSTARTED INT DEFAULT 0;
    RESTORE_STMT VARCHAR;
    TDATE DATE DEFAULT :SDATE;
BEGIN
    CREATE TEMP TABLE PENDING (NAME VARCHAR, RESP VARCHAR);
    WHILE (TDATE <= EDATE) DO
        RESTORE_STMT := 'LS @my_ext_stage pattern=\'EXAMPLE_DATA\\' || TDATE || '.*\'';
        EXECUTE IMMEDIATE :RESTORE_STMT;
        INSERT INTO PENDING SELECT "name", RESTORE_FILE('summit24-glacier', REPLACE("name",'s3://summit24-glacier/',''), :DAYS) as RESPONSE 
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE RESPONSE != 'Restored';
        TOTALSTARTED := TOTALSTARTED + SQLROWCOUNT;  
        TDATE := TDATE + 1;
    END WHILE;
    DROP TABLE PENDING;
    RETURN TOTALSTARTED;
END;

SET (S, E) = (SELECT DATE_FROM_PARTS(2024,1,1), DATE_FROM_PARTS(2024,1,1));
CALL RESTORE_EXAMPLE_DAYS($S, $E, 7);

CREATE STAGE my_ext_stage
  URL='s3://summit24-glacier'
  CREDENTIALS=(AWS_KEY_ID=$ACCESS_KEY AWS_SECRET_KEY=$SECRET_ACCESS_KEY);

CREATE OR REPLACE EXTERNAL TABLE RESTORED_EXAMPLE_DATA(
	ID VARCHAR(36) as (value:ID::varchar),
    NUM_1 NUMBER(2,0) as (value:NUM_1::int),
	NUM_2 NUMBER(7,0) as (value:NUM_2::int),
	STR_1 VARCHAR(16777216) as (value:STR_1::varchar),
    EVT_DATE DATE as (parse_json(metadata$external_table_partition):EVT_DATE::date),
    EVT_TIME TIME as (value:EVT_TIME::time)
) partition by (EVT_DATE) 
location=@my_ext_stage/EXAMPLE_DATA
partition_type = user_specified file_format= (type=parquet);

ALTER EXTERNAL TABLE RESTORED_EXAMPLE_DATA ADD PARTITION(EVT_DATE='2024-01-01') LOCATION '2024-01-01';

SELECT * EXCLUDE(VALUE) FROM RESTORED_EXAMPLE_DATA;

