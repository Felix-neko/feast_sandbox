DROP DATABASE housing CASCADE;

CREATE DATABASE housing;

USE housing;

CREATE TABLE house
(
    house_id BIGINT,
    name STRING,
    age BIGINT,
    price DOUBLE,
    floor_area DOUBLE,
    event_timestamp TIMESTAMP
);

CREATE TABLE buyer
(
    buyer_id BIGINT,
    name STRING,
    age BIGINT,
    yearly_income DOUBLE,
    event_timestamp TIMESTAMP
);

CREATE TABLE mortgage
(
    mortgage_id BIGINT,
    name STRING,
    purchase_date DATE,
    due_date DATE,
    value DOUBLE,
    house_id BIGINT,
    buyer_id BIGINT,
    event_timestamp TIMESTAMP
);

INSERT INTO buyer VALUES
(1, 'Jesus Christ', 33, 0, '2000-01-01'),
(2, 'Vladimir Lenin', 54, 33000, '2001-01-01'),
(3, 'Joseph Steelman', 74, 77400, '2000-02-02'),
(4, 'Angela Davis', 45, 140000, '2005-03-05')
;

INSERT INTO house VALUES
(1, 'Winter Palace', 150, 1e6, 2e3, '1750-05-22'),
(2, 'Uncle Tom''s Cabin', 20, 200, 12, '1840-12-31'),
(3, 'The House Jack Built', 40, 2000, 30, '1920-10-10')
;

INSERT INTO mortgage VALUES
(1, '', '1994-12-31', '2004-12-31', 100000, 1, 2, '2020-11-01'),
(2, '', '1996-11-07', '2007-05-01', 10000, 2, 2, '2020-11-01')
;