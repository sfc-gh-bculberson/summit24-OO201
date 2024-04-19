CREATE OR REPLACE SECRET my_aws_access_key_iam  
TYPE = GENERIC_STRING
SECRET_STRING = $ACCESS_KEY;

CREATE OR REPLACE SECRET my_aws_secret_key_iam  
TYPE = GENERIC_STRING
SECRET_STRING = $SECRET_ACCESS_KEY;

CREATE OR REPLACE NETWORK RULE aws_apis_network_rule  
MODE = EGRESS  
TYPE = HOST_PORT
VALUE_LIST = ('summit24-glacier.s3.amazonaws.com:443', 'summit24-glacier.s3.us-west-2.amazonaws.com:443');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION aws_apis_access_integration  
ALLOWED_NETWORK_RULES = (aws_apis_network_rule)  
ALLOWED_AUTHENTICATION_SECRETS = (my_aws_access_key_iam, my_aws_secret_key_iam)  
ENABLED = true;

CREATE OR REPLACE FUNCTION ARCHIVE_FILE(file_path string, file_name string, storage_class string)
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
from snowflake.snowpark.files import SnowflakeFile

def run(file_path, file_name, storage_class):
    k = _snowflake.get_generic_secret_string('aws_iam_key')
    s = _snowflake.get_generic_secret_string('aws_iam_secret')
    s3_client = boto3.client('s3', aws_access_key_id=k, aws_secret_access_key=s)
    with SnowflakeFile.open(file_path, 'rb') as f:
        s3_client.upload_fileobj(f, 'summit24-glacier', file_name, ExtraArgs={'StorageClass': storage_class})
        return 'OK'
$$;

CREATE OR REPLACE PROCEDURE ARCHIVE_EXAMPLE_DATA(ENDDATE DATE, STORAGECLASS VARCHAR)
RETURNS TEXT
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
    CREATE OR REPLACE TEMPORARY STAGE TMP;
    COPY INTO @TMP FROM
    (SELECT * FROM LIFECYCLE.DATA.EXAMPLE_DATA WHERE EVT_DATE < :ENDDATE)
    PARTITION BY CAST(EVT_DATE AS VARCHAR) FILE_FORMAT=(TYPE=PARQUET) HEADER=true DETAILED_OUTPUT=true;
    
    SELECT ARCHIVE_FILE(build_scoped_file_url(@TMP, FILE_NAME), CONCAT('EXAMPLE_DATA/', FILE_NAME), :STORAGECLASS) 
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    
    DELETE FROM LIFECYCLE.DATA.EXAMPLE_DATA WHERE EVT_DATE < :ENDDATE;
    DROP STAGE TMP;
    RETURN 'Done';
END;

CREATE OR REPLACE TASK ARCHIVE_EXAMPLE_DATA
SCHEDULE = 'USING CRON 0 0 1 * * UTC'
WAREHOUSE = 'DEFAULT'
AS
BEGIN
    CALL ARCHIVE_EXAMPLE_DATA(DATEADD(day, -30, CURRENT_DATE()),'DEEP_ARCHIVE');
END;
ALTER TASK ARCHIVE_EXAMPLE_DATA RESUME;

EXECUTE TASK ARCHIVE_EXAMPLE_DATA;
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME DESC;

CALL ARCHIVE_EXAMPLE_DATA(DATEADD(day, -30, CURRENT_DATE()),'STANDARD');

