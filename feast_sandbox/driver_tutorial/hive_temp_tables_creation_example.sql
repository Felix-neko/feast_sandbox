USE driver_tutorial;



CREATE TABLE feast_entity_df_temp (
  event_timestamp timestamp, driver_id bigint, trip_completed bigint
)
STORED AS PARQUET;


INSERT INTO TABLE feast_entity_df_temp VALUES
    ("2021-04-16 20:29:28.000000", 1001, 1), ("2021-04-17 04:29:28.000000", 1002, 0),
    ("2021-04-17 12:29:28.000000", 1003, 0), ("2021-04-17 20:29:28.000000", 1001, 1),
    ("2021-04-18 04:29:28.000000", 1002, 0), ("2021-04-18 12:29:28.000000", 1003, 0),
    ("2021-04-18 20:29:28.000000", 1001, 1), ("2021-04-19 04:29:28.000000", 1002, 0),
    ("2021-04-19 12:29:28.000000", 1003, 0), ("2021-04-19 20:29:28.000000", 1004, 1);



SET hive.strict.checks.cartesian.product=false;

SET hive.support.quoted.identifiers=None;

SET hive.exec.temporary.table.storage=memory;


CREATE TEMPORARY TABLE entity_dataframe AS (
    SELECT *,
           event_timestamp AS entity_timestamp
            ,
           CONCAT(
                   CAST(driver_id AS STRING),
                   CAST(event_timestamp AS STRING)
               )           AS driver_hourly_stats__entity_row_unique_id

    FROM feast_entity_df_temp
);


-- Start create temporary table *__base


CREATE TEMPORARY TABLE driver_hourly_stats__base AS
WITH driver_hourly_stats__entity_dataframe AS (
    SELECT
        driver_id,
        entity_timestamp,
        driver_hourly_stats__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY driver_id, entity_timestamp, driver_hourly_stats__entity_row_unique_id
),

/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously

 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/

driver_hourly_stats__subquery AS (
    SELECT
        event_timestamp as event_timestamp,
        created_timestamp as created_timestamp,
        driver_id AS driver_id,

            conv_rate as conv_rate,

            acc_rate as acc_rate,

            avg_daily_trips as avg_daily_trips

    FROM (
    SELECT event_timestamp, driver_id, conv_rate, acc_rate, avg_daily_trips, created_timestamp
    FROM driver_stat
    ) AS subquery
    INNER JOIN (
        SELECT MAX(entity_timestamp) as max_entity_timestamp_

               ,(MIN(entity_timestamp) - interval '315360000' second) as min_entity_timestamp_

        FROM entity_dataframe
    ) AS temp
    ON (
        event_timestamp <= max_entity_timestamp_

        AND event_timestamp >=  min_entity_timestamp_

    )
)
SELECT
    subquery.*,
    entity_dataframe.entity_timestamp,
    entity_dataframe.driver_hourly_stats__entity_row_unique_id
FROM driver_hourly_stats__subquery AS subquery
INNER JOIN (
    SELECT *

    , (entity_timestamp - interval '315360000' second) as ttl_entity_timestamp

    FROM driver_hourly_stats__entity_dataframe
) AS entity_dataframe
ON (
    subquery.event_timestamp <= entity_dataframe.entity_timestamp


    AND subquery.event_timestamp >= entity_dataframe.ttl_entity_timestamp



    AND subquery.driver_id = entity_dataframe.driver_id

);


-- End create temporary table *__base




WITH

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/

driver_hourly_stats__dedup AS (
    SELECT
        driver_hourly_stats__entity_row_unique_id,
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM driver_hourly_stats__base
    GROUP BY driver_hourly_stats__entity_row_unique_id, event_timestamp
),


/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
driver_hourly_stats__latest AS (
    SELECT
        base.driver_hourly_stats__entity_row_unique_id,
        MAX(base.event_timestamp) AS event_timestamp

            ,MAX(base.created_timestamp) AS created_timestamp


    FROM driver_hourly_stats__base AS base

        INNER JOIN driver_hourly_stats__dedup AS dedup
        ON (
            dedup.driver_hourly_stats__entity_row_unique_id=base.driver_hourly_stats__entity_row_unique_id
            AND dedup.event_timestamp=base.event_timestamp
            AND dedup.created_timestamp=base.created_timestamp
        )


    GROUP BY base.driver_hourly_stats__entity_row_unique_id
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
driver_hourly_stats__cleaned AS (
    SELECT base.*
    FROM driver_hourly_stats__base AS base
    INNER JOIN driver_hourly_stats__latest AS latest
    ON (
        base.driver_hourly_stats__entity_row_unique_id=latest.driver_hourly_stats__entity_row_unique_id
        AND base.event_timestamp=latest.event_timestamp

            AND base.created_timestamp=latest.created_timestamp

    )
)




/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT `(entity_timestamp|driver_hourly_stats__entity_row_unique_id)?+.+`
FROM entity_dataframe

LEFT JOIN (
    SELECT
        driver_hourly_stats__entity_row_unique_id

            ,conv_rate

            ,acc_rate

            ,avg_daily_trips

    FROM driver_hourly_stats__cleaned
) AS driver_hourly_stats__joined
ON (
    driver_hourly_stats__joined.driver_hourly_stats__entity_row_unique_id=entity_dataframe.driver_hourly_stats__entity_row_unique_id
);