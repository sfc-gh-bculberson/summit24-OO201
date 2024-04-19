CREATE DATABASE LIFECYCLE;
CREATE SCHEMA DATA;
SET SECONDS = (
        SELECT DATEDIFF(
                second,
                DATE_FROM_PARTS(2024, 1, 1),
                CURRENT_DATE()
            )
    );
CREATE OR REPLACE ICEBERG TABLE EXAMPLE_DATA (
        ID STRING,
        NUM_1 NUMBER(2, 0),
        NUM_2 NUMBER(7, 0),
        STR_1 STRING,
        EVT_TIME TIME,
        EVT_DATE DATE
    ) EXTERNAL_VOLUME = 'summit24' CATALOG = 'SNOWFLAKE' BASE_LOCATION = 'EXAMPLE_DATA' AS
SELECT UUID_STRING() as ID,
    uniform(1, 10, RANDOM(12)) as NUM_1,
    uniform(1, 1000000, RANDOM(12)) as NUM_2,
    randstr(255, random()) as STR_1,
    TO_TIME(
        dateadd(second, seq4(), DATE_FROM_PARTS(2024, 1, 1))
    ) as EVT_TIME,
    TO_DATE(
        dateadd(second, seq4(), DATE_FROM_PARTS(2024, 1, 1))
    ) as EVT_DATE
FROM TABLE(GENERATOR(ROWCOUNT => $SECONDS));

