-- Here we create two databases that have a table with same name in both databases.
-- feast-hive will crash trying to find tables in them even with database name set

CREATE DATABASE IF NOT EXISTS housing;

USE housing;

CREATE TABLE house
(
    house_id BIGINT,
    name STRING,
    age BIGINT,
    price DOUBLE,
    floor_area DOUBLE,
    event_timestamp TIMESTAMP
) STORED AS PARQUET;

-- Winter Palace's price is growing
INSERT INTO house VALUES
(1, 'Winter Palace', 150, 1e6, 2e3, '1985-05-22'),
(1, 'Winter Palace', 151, 2e6, 2e3, '1986-05-22'),
(1, 'Winter Palace', 153, 3e6, 2e3, '1987-05-22'),
(2, 'Uncle Tom''s Cabin', 20, 200, 12, '1994-12-31'),
(3, 'The House Jack Built', 40, 2000, 30, '1998-10-10')
;

CREATE DATABASE IF NOT EXISTS housing2;

USE housing2;

CREATE TABLE house
(
    house_id BIGINT,
    name STRING,
    age BIGINT,
    price DOUBLE,
    floor_area DOUBLE,
    event_timestamp TIMESTAMP
) STORED AS PARQUET;

-- Winter Palace's price is growing
INSERT INTO house VALUES
(1, 'Winter Palace', 150, 1e6, 2e3, '1985-05-22'),
(1, 'Winter Palace', 151, 2e6, 2e3, '1986-05-22'),
(1, 'Winter Palace', 153, 3e6, 2e3, '1987-05-22'),
(2, 'Uncle Tom''s Cabin', 20, 200, 12, '1994-12-31'),
(3, 'The House Jack Built', 40, 2000, 30, '1998-10-10')
;
