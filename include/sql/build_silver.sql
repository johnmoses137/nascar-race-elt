MERGE INTO NASCAR_RACE_ELT.SILVER.RACES AS target
USING (
    SELECT
        raw_data:weekend_race[0]:race_id::NUMBER AS race_id,
        raw_data:weekend_race[0]:race_season::NUMBER AS race_season,
        raw_data:weekend_race[0]:race_name::VARCHAR AS race_name,
        TO_DATE(LEFT(raw_data:weekend_race[0]:race_date::VARCHAR, 10)) AS race_date,
        raw_data:weekend_race[0]:track_name::VARCHAR AS track_name,
        raw_data:weekend_race[0]:number_of_cars_in_field::NUMBER
            AS number_of_cars_in_field
    FROM NASCAR_RACE_ELT.BRONZE.RAW_NASCAR_JSON
    WHERE raw_data:weekend_race[0]:race_id IS NOT NULL
) AS source
ON target.race_id = source.race_id
WHEN MATCHED THEN UPDATE ALL BY NAME
WHEN NOT MATCHED THEN INSERT ALL BY NAME;


MERGE INTO NASCAR_RACE_ELT.SILVER.RACE_RESULTS AS target
USING (
    SELECT
        b.raw_data:weekend_race[0]:race_id::NUMBER AS race_id,
        r.value:driver_id::NUMBER AS driver_id,
        r.value:driver_fullname::VARCHAR AS driver_name,
        r.value:starting_position::NUMBER AS starting_position,
        r.value:finishing_position::NUMBER AS finishing_position,
        r.value:laps_completed::NUMBER AS laps_completed,
        r.value:finishing_status::VARCHAR AS finishing_status
    FROM NASCAR_RACE_ELT.BRONZE.RAW_NASCAR_JSON AS b,
    LATERAL FLATTEN(
        INPUT => b.raw_data:weekend_race[0]:results
    ) AS r
) AS source
ON target.race_id = source.race_id
AND target.driver_id = source.driver_id
WHEN MATCHED THEN UPDATE ALL BY NAME
WHEN NOT MATCHED THEN INSERT ALL BY NAME;