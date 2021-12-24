USE housing;

--CREATE_ENTITY_TABLE_QUERY

CREATE TABLE feast_entity_df_852b2d7ff3c343199d0e259030333807 (
  house_id bigint, buyer_id bigint, event_timestamp timestamp
)
STORED AS PARQUET;

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

    FROM feast_entity_df_852b2d7ff3c343199d0e259030333807
;
--DROP_TABLE_QUERY:
DROP TABLE IF EXISTS feast_entity_df_852b2d7ff3c343199d0e259030333807;