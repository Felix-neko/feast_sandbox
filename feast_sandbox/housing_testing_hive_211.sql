--CREATE_ENTITY_TABLE_QUERY

USE housing2;

SET hive.strict.checks.cartesian.product = false;

CREATE TABLE feast_entity_df_bae6596ef29241ed8366943d0b3294f5 (
  house_id bigint, buyer_id bigint, event_timestamp timestamp
)
STORED AS PARQUET;
            
-- ENTITY_CHUNK_INSERT

INSERT INTO TABLE feast_entity_df_bae6596ef29241ed8366943d0b3294f5
VALUES
    (1, 2, "2015-11-08 00:00:00.000000"),
    (2, 2, "2018-01-01 00:00:00.000000"),
    (3, 2, "2019-01-01 00:00:00.000000")
    ;
            
--TO_ARROW_QUERY:
SET hive.mapred.mode=nonstrict;
--TO_ARROW_QUERY:
SET hive.support.quoted.identifiers=none;
--TO_ARROW_QUERY:
SET hive.resultset.use.unique.column.names=false;
--TO_ARROW_QUERY:
SET hive.exec.temporary.table.storage=memory;

SET hive.strict.checks.cartesian.product=false;


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



CREATE TABLE suitable__house_rows AS
    SELECT
        house.house_id AS house_id,
        MAX(house.event_timestamp) AS max_event_ts,
        entity_dataframe.house__entity_row_unique_id AS house_row_unique_id
    FROM house, entity_dataframe
    WHERE
        house.house_id = entity_dataframe.house_id
        AND house.event_timestamp <= entity_dataframe.entity_timestamp
        AND house.event_timestamp >= entity_dataframe.entity_timestamp - INTERVAL '946080000' SECOND
    GROUP BY house.house_id, entity_dataframe.house__entity_row_unique_id;

CREATE TABLE suitable__house AS
WITH suitable__house_rows AS
(
    SELECT
        house.house_id AS house_id,
        MAX(house.event_timestamp) AS max_event_ts,
        entity_dataframe.house__entity_row_unique_id AS house_row_unique_id
    FROM house, entity_dataframe
    WHERE
        house.house_id = entity_dataframe.house_id
        AND house.event_timestamp <= entity_dataframe.entity_timestamp
        AND house.event_timestamp >= entity_dataframe.entity_timestamp - INTERVAL '946080000' SECOND
    GROUP BY house.house_id, entity_dataframe.house__entity_row_unique_id
)

    SELECT house.*, house_row_unique_id FROM house, suitable__house_rows
    WHERE
        house.house_id = suitable__house_rows.house_id
        AND house.event_timestamp = suitable__house_rows.max_event_ts
;


CREATE TABLE suitable__buyer AS
WITH suitable__buyer_rows AS
(
    SELECT
        buyer.buyer_id AS buyer_id,
        MAX(buyer.event_timestamp) AS max_event_ts,
        entity_dataframe.buyer__entity_row_unique_id AS buyer_row_unique_id
    FROM buyer, entity_dataframe
    WHERE
        buyer.buyer_id = entity_dataframe.buyer_id
        AND buyer.event_timestamp <= entity_dataframe.entity_timestamp
        AND buyer.event_timestamp >= entity_dataframe.entity_timestamp - INTERVAL '946080000' SECOND
    GROUP BY buyer.buyer_id, entity_dataframe.buyer__entity_row_unique_id
)

    SELECT buyer.*, buyer_row_unique_id FROM buyer, suitable__buyer_rows
    WHERE
        buyer.buyer_id = suitable__buyer_rows.buyer_id
        AND buyer.event_timestamp = suitable__buyer_rows.max_event_ts
;


CREATE TABLE suitable__mortgage AS
WITH suitable__mortgage_rows AS
(
    SELECT
        mortgage.house_id AS house_id,
        mortgage.buyer_id AS buyer_id,
        MAX(mortgage.event_timestamp) AS max_event_ts,
        entity_dataframe.mortgage__entity_row_unique_id AS mortgage_row_unique_id
    FROM mortgage, entity_dataframe
    WHERE
        mortgage.house_id = entity_dataframe.house_id
        AND mortgage.buyer_id = entity_dataframe.buyer_id
        AND mortgage.event_timestamp <= entity_dataframe.entity_timestamp
        AND mortgage.event_timestamp >= entity_dataframe.entity_timestamp - INTERVAL '946080000' SECOND
    GROUP BY mortgage.house_id, mortgage.buyer_id, entity_dataframe.mortgage__entity_row_unique_id
)

    SELECT mortgage.*, mortgage_row_unique_id FROM mortgage, suitable__mortgage_rows
    WHERE
        mortgage.house_id = suitable__mortgage_rows.house_id
        AND mortgage.buyer_id = suitable__mortgage_rows.buyer_id
        AND mortgage.event_timestamp = suitable__mortgage_rows.max_event_ts
;

SELECT entity_dataframe.event_timestamp AS entity_event_timestamp, entity_dataframe.house__entity_row_unique_id AS edf_house_row_id, suitable__mortgage.*
    FROM entity_dataframe
        LEFT OUTER JOIN suitable__house ON entity_dataframe.house__entity_row_unique_id = suitable__house.house_row_unique_id
        LEFT OUTER JOIN suitable__buyer ON entity_dataframe.buyer__entity_row_unique_id = suitable__buyer.buyer_row_unique_id
        LEFT OUTER JOIN suitable__mortgage ON entity_dataframe.mortgage__entity_row_unique_id = suitable__mortgage.mortgage_row_unique_id
;