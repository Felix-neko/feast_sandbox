CREATE DATABASE IF NOT EXISTS timestamp_checkup;

USE timestamp_checkup;

CREATE TABLE IF NOT EXISTS daemon;

INSERT INTO daemon (name, date_of_birth, event_timestamp, created_timestamp) VALUES
    ('Belzebub', '1991-12-23 18:30:00.000000000', '2001-09-10 18:30:00.000000000', '2007-12-31 18:30:00.000000000'),
    ('Sathanail', '1992-12-30 18:30:00.000000000', '2002-09-14 18:30:00.000000000', '2008-12-31 18:30:00.000000000')
;