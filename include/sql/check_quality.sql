-- Return race IDs referenced by driver results but missing from RACES.
SELECT
    rr.race_id,
    COUNT(*) AS unmatched_driver_results
FROM NASCAR_RACE_ELT.SILVER.RACE_RESULTS AS rr
LEFT JOIN NASCAR_RACE_ELT.SILVER.RACES AS r
    ON rr.race_id = r.race_id
WHERE r.race_id IS NULL
GROUP BY rr.race_id;


-- Return driver results with invalid finishing positions or field sizes.
SELECT
    rr.race_id,
    rr.driver_id,
    rr.finishing_position,
    r.number_of_cars_in_field
FROM NASCAR_RACE_ELT.SILVER.RACE_RESULTS AS rr
JOIN NASCAR_RACE_ELT.SILVER.RACES AS r
    ON rr.race_id = r.race_id
WHERE rr.finishing_position IS NULL
   OR r.number_of_cars_in_field IS NULL
   OR r.number_of_cars_in_field <= 0
   OR rr.finishing_position < 0
   OR rr.finishing_position > r.number_of_cars_in_field;


   -- Return races whose driver-result counts differ between Bronze and Silver.
WITH bronze_counts AS (
    SELECT
        b.raw_data:weekend_race[0]:race_id::NUMBER AS race_id,
        COUNT(*) AS bronze_rows
    FROM NASCAR_RACE_ELT.BRONZE.RAW_NASCAR_JSON AS b,
    LATERAL FLATTEN(
        INPUT => b.raw_data:weekend_race[0]:results
    ) AS result
    GROUP BY 1
),
silver_counts AS (
    SELECT race_id, COUNT(*) AS silver_rows
    FROM NASCAR_RACE_ELT.SILVER.RACE_RESULTS
    GROUP BY race_id
)
SELECT
    COALESCE(b.race_id, s.race_id) AS race_id,
    COALESCE(b.bronze_rows, 0) AS bronze_rows,
    COALESCE(s.silver_rows, 0) AS silver_rows
FROM bronze_counts AS b
FULL OUTER JOIN silver_counts AS s ON b.race_id = s.race_id
WHERE COALESCE(b.bronze_rows, 0) <> COALESCE(s.silver_rows, 0);

-- Return drivers with more than one result for the same race.
SELECT
    race_id,
    driver_id,
    COUNT(*) AS result_count
FROM NASCAR_RACE_ELT.SILVER.RACE_RESULTS
GROUP BY race_id, driver_id
HAVING COUNT(*) > 1;


-- Return events with a missing or unexpected race type.
SELECT
    event.value:race_id::NUMBER AS race_id,
    event.value:race_name::VARCHAR AS race_name,
    event.value:race_type_id AS race_type_id
FROM NASCAR_RACE_ELT.BRONZE.RAW_NASCAR_JSON AS b,
LATERAL FLATTEN(INPUT => b.raw_data) AS event
WHERE b.source_file LIKE '%race_list_2025.json%'
  AND (
      TRY_TO_NUMBER(event.value:race_type_id::VARCHAR) IS NULL
      OR event.value:race_type_id::VARCHAR NOT IN ('1', '2')
  );
