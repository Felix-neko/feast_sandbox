--CREATE_ENTITY_TABLE_QUERY

USE housing;

CREATE TABLE feast_entity_df_bae6596ef29241ed8366943d0b3294f5 (
  house_id bigint, buyer_id bigint, event_timestamp timestamp
)
STORED AS PARQUET;
            
-- ENTITY_CHUNK_INSERT

INSERT INTO TABLE feast_entity_df_bae6596ef29241ed8366943d0b3294f5
VALUES (1, 2, "2015-11-08 00:00:00.000000"), (2, 2, "2018-01-01 00:00:00.000000");
            
--TO_ARROW_QUERY:
SET hive.mapred.mode=nonstrict;
--TO_ARROW_QUERY:
SET hive.support.quoted.identifiers=none;
--TO_ARROW_QUERY:
SET hive.resultset.use.unique.column.names=false;
--TO_ARROW_QUERY:
SET hive.exec.temporary.table.storage=memory;
--TO_ARROW_QUERY:

--
-- Compute a deterministic hash for the `left_table_query_string` that will be used throughout
-- all the logic as the field to GROUP BY the data
--

CREATE TABLE entity_dataframe AS
    SELECT *,
        event_timestamp AS entity_timestamp
            ,CONCAT(
                    CAST(house_id AS STRING),
                CAST(event_timestamp AS STRING)
            ) AS house__entity_row_unique_id
            ,CONCAT(
                    CAST(buyer_id AS STRING),
                CAST(event_timestamp AS STRING)
            ) AS buyer__entity_row_unique_id
            ,CONCAT(
                CAST(buyer_id AS STRING),
                CAST(house_id AS STRING),
                CAST(event_timestamp AS STRING)
            ) AS mortgage__entity_row_unique_id
    FROM feast_entity_df_bae6596ef29241ed8366943d0b3294f5;
--TO_ARROW_QUERY:

-- Start create temporary table *__base

CREATE TABLE house__base AS
WITH house__entity_dataframe AS (
    SELECT
        house_id,
        entity_timestamp,
        house__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY house_id, entity_timestamp, house__entity_row_unique_id
),

--
-- This query template performs the point-in-time correctness join for a single feature set table
-- to the provided entity table.
--
-- 1. We first join the current feature_view to the entity dataframe that has been passed.
-- This JOIN has the following logic:
--    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
--    is less than the one provided in the entity dataframe
--    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
--    is higher the the one provided minus the TTL
--    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
--    computed previously
--
-- The output of this CTE will contain all the necessary information and already filtered out most
-- of the data that is not relevant.
--

house__subquery AS (
    SELECT
        event_timestamp as event_timestamp,
        house_id AS house_id,
        age as house__age,
        name as house__name
    FROM `house` AS subquery
    INNER JOIN (
        SELECT MAX(entity_timestamp) as max_entity_timestamp_
           ,(MIN(entity_timestamp) - interval '946080000' second) as min_entity_timestamp_
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
    entity_dataframe.house__entity_row_unique_id
FROM house__subquery AS subquery
INNER JOIN (
    SELECT *, (entity_timestamp - interval '946080000' second) as ttl_entity_timestamp
    FROM house__entity_dataframe
) AS entity_dataframe
ON ( 
    subquery.event_timestamp <= entity_dataframe.entity_timestamp
    AND subquery.event_timestamp >= entity_dataframe.ttl_entity_timestamp
    AND subquery.house_id = entity_dataframe.house_id
);
--TO_ARROW_QUERY:

CREATE TABLE buyer__base AS
WITH buyer__entity_dataframe AS (
    SELECT
        buyer_id,
        entity_timestamp,
        buyer__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY buyer_id, entity_timestamp, buyer__entity_row_unique_id
),

--
-- This query template performs the point-in-time correctness join for a single feature set table
-- to the provided entity table.
--
-- 1. We first join the current feature_view to the entity dataframe that has been passed.
-- This JOIN has the following logic:
--    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
--    is less than the one provided in the entity dataframe
--    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
--    is higher the the one provided minus the TTL
--    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
--    computed previously
--
-- The output of this CTE will contain all the necessary information and already filtered out most
-- of the data that is not relevant.
--

buyer__subquery AS (
    SELECT
        event_timestamp as event_timestamp,
        buyer_id AS buyer_id,
        age as buyer__age,
        name as buyer__name,
        yearly_income as buyer__yearly_income
    FROM `buyer` AS subquery
    INNER JOIN (
        SELECT MAX(entity_timestamp) as max_entity_timestamp_
           ,(MIN(entity_timestamp) - interval '946080000' second) as min_entity_timestamp_
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
    entity_dataframe.buyer__entity_row_unique_id
FROM buyer__subquery AS subquery
INNER JOIN (
    SELECT *
    , (entity_timestamp - interval '946080000' second) as ttl_entity_timestamp
    FROM buyer__entity_dataframe
) AS entity_dataframe
ON ( 
    subquery.event_timestamp <= entity_dataframe.entity_timestamp
    AND subquery.event_timestamp >= entity_dataframe.ttl_entity_timestamp
    AND subquery.buyer_id = entity_dataframe.buyer_id
);
--TO_ARROW_QUERY:

CREATE TABLE mortgage__base AS
WITH mortgage__entity_dataframe AS (
    SELECT
        buyer_id, house_id,
        entity_timestamp,
        mortgage__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY buyer_id, house_id, entity_timestamp, mortgage__entity_row_unique_id
),

--
-- This query template performs the point-in-time correctness join for a single feature set table
-- to the provided entity table.
--
-- 1. We first join the current feature_view to the entity dataframe that has been passed.
-- This JOIN has the following logic:
--    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
--    is less than the one provided in the entity dataframe
--    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
--    is higher the the one provided minus the TTL
--    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
--    computed previously
--
-- The output of this CTE will contain all the necessary information and already filtered out most
-- of the data that is not relevant.
--

mortgage__subquery AS (
    SELECT
        event_timestamp as event_timestamp,
        buyer_id AS buyer_id, house_id AS house_id,
        value as mortgage__value
    FROM `mortgage` AS subquery
    INNER JOIN (
        SELECT MAX(entity_timestamp) as max_entity_timestamp_,
           (MIN(entity_timestamp) - interval '946080000' second) as min_entity_timestamp_
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
    entity_dataframe.mortgage__entity_row_unique_id
FROM mortgage__subquery AS subquery
INNER JOIN (
    SELECT *, (entity_timestamp - interval '946080000' second) as ttl_entity_timestamp
    FROM mortgage__entity_dataframe
) AS entity_dataframe
ON ( 
    subquery.event_timestamp <= entity_dataframe.entity_timestamp
    AND subquery.event_timestamp >= entity_dataframe.ttl_entity_timestamp
    AND subquery.buyer_id = entity_dataframe.buyer_id
    AND subquery.house_id = entity_dataframe.house_id
);


--TO_ARROW_QUERY:

-- End create temporary table *__base

WITH
--
-- 2. If the `created_timestamp_column` has been set, we need to
-- deduplicate the data first. This is done by calculating the
-- `MAX(created_at_timestamp)` for each event_timestamp.
-- We then join the data on the next CTE
--


--
-- 3. The data has been filtered during the first CTE "*__base"
-- Thus we only need to compute the latest timestamp of each feature.
--
house__latest AS (
    SELECT
        base.house__entity_row_unique_id,
        MAX(base.event_timestamp) AS event_timestamp
    FROM house__base AS base
    GROUP BY base.house__entity_row_unique_id
),

--
-- 4. Once we know the latest value of each feature for a given timestamp,
-- we can join again the data back to the original "base" dataset
--
house__cleaned AS (
    SELECT base.*
    FROM house__base AS base
    INNER JOIN house__latest AS latest
    ON (
        base.house__entity_row_unique_id=latest.house__entity_row_unique_id
        AND base.event_timestamp=latest.event_timestamp
    )
), 

--
-- 2. If the `created_timestamp_column` has been set, we need to
-- deduplicate the data first. This is done by calculating the
-- `MAX(created_at_timestamp)` for each event_timestamp.
-- We then join the data on the next CTE
--


--
-- 3. The data has been filtered during the first CTE "*__base"
-- Thus we only need to compute the latest timestamp of each feature.
--
buyer__latest AS (
    SELECT
        base.buyer__entity_row_unique_id,
        MAX(base.event_timestamp) AS event_timestamp
    FROM buyer__base AS base
    GROUP BY base.buyer__entity_row_unique_id
),

--
-- 4. Once we know the latest value of each feature for a given timestamp,
-- we can join again the data back to the original "base" dataset
--
buyer__cleaned AS (
    SELECT base.*
    FROM buyer__base AS base
    INNER JOIN buyer__latest AS latest
    ON (
        base.buyer__entity_row_unique_id=latest.buyer__entity_row_unique_id
        AND base.event_timestamp=latest.event_timestamp
    )
), 






--
-- 2. If the `created_timestamp_column` has been set, we need to
-- deduplicate the data first. This is done by calculating the
-- `MAX(created_at_timestamp)` for each event_timestamp.
-- We then join the data on the next CTE
--


--
-- 3. The data has been filtered during the first CTE "*__base"
-- Thus we only need to compute the latest timestamp of each feature.
--
mortgage__latest AS (
    SELECT
        base.mortgage__entity_row_unique_id,
        MAX(base.event_timestamp) AS event_timestamp
        

    FROM mortgage__base AS base
    

    GROUP BY base.mortgage__entity_row_unique_id
),

--
-- 4. Once we know the latest value of each feature for a given timestamp,
-- we can join again the data back to the original "base" dataset
--
mortgage__cleaned AS (
    SELECT base.*
    FROM mortgage__base AS base
    INNER JOIN mortgage__latest AS latest
    ON (
        base.mortgage__entity_row_unique_id=latest.mortgage__entity_row_unique_id
        AND base.event_timestamp=latest.event_timestamp
        
    )
)




--
-- Joins the outputs of multiple time travel joins to a single table.
-- The entity_dataframe dataset being our source of truth here.
--

SELECT `(entity_timestamp|house__entity_row_unique_id|buyer__entity_row_unique_id|mortgage__entity_row_unique_id)?+.+`
FROM entity_dataframe

LEFT JOIN (
    SELECT
        house__entity_row_unique_id
        
            ,house__age
        
            ,house__name
        
    FROM house__cleaned
) AS house__joined 
ON (
    house__joined.house__entity_row_unique_id=entity_dataframe.house__entity_row_unique_id
)

LEFT JOIN (
    SELECT
        buyer__entity_row_unique_id
        
            ,buyer__age
        
            ,buyer__name
        
            ,buyer__yearly_income
        
    FROM buyer__cleaned
) AS buyer__joined 
ON (
    buyer__joined.buyer__entity_row_unique_id=entity_dataframe.buyer__entity_row_unique_id
)

LEFT JOIN (
    SELECT
        mortgage__entity_row_unique_id
        
            ,mortgage__value
        
    FROM mortgage__cleaned
) AS mortgage__joined 
ON (
    mortgage__joined.mortgage__entity_row_unique_id=entity_dataframe.mortgage__entity_row_unique_id
);
