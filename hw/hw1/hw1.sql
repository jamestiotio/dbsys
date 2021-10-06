-- STUDENT number. UPDATE ONLY, DO NOT DELETE. 
select "1004555";
-- Replace the above with your student number

-- DATA LOADING Separator. DO NOT DELETE
select "DATA LOADING";
-- Put your table creation and data loading SQL statements here
drop database if exists `air_travel`;
create database if not exists `air_travel`;
use `air_travel`;
drop table if exists `weekdays`;
drop table if exists `months`;
drop table if exists `carriers`;
drop table if exists `flights`;
create table if not exists `weekdays` (
    `did` INTEGER PRIMARY KEY,
    `day_of_week` TEXT NOT NULL
);
create table if not exists `months` (
    `mid` INTEGER PRIMARY KEY,
    `month` TEXT NOT NULL
);
create table if not exists `carriers` (
    `cid` VARCHAR(7) PRIMARY KEY,
    `name` TEXT NOT NULL
);
create table if not exists `flights` (
    `fid` INTEGER PRIMARY KEY,
    `year` INTEGER,
    `month_id` INTEGER,
    `day_of_month` INTEGER,
    `day_of_week_id` INTEGER,
    `carrier_id` VARCHAR(7),
    `flight_num` INTEGER,
    `origin_city` TEXT,
    `origin_state` TEXT,
    `dest_city` TEXT,
    `dest_state` TEXT,
    `departure_delay` DOUBLE,
    `taxi_out` DOUBLE,
    `arrival_delay` DOUBLE,
    `cancelled` SMALLINT,
    `actual_time` DOUBLE,
    `distance` DOUBLE,
    FOREIGN KEY (`month_id`) REFERENCES `months`(`mid`),
    FOREIGN KEY (`day_of_week_id`) REFERENCES `weekdays`(`did`),
    FOREIGN KEY (`carrier_id`) REFERENCES `carriers`(`cid`)
);
load data infile "/tmp/sql/weekdays.csv" into table `weekdays` fields terminated by "," enclosed by '"' lines terminated by "\n";
load data infile "/tmp/sql/months.csv" into table `months` fields terminated by "," enclosed by '"' lines terminated by "\n";
load data infile "/tmp/sql/carriers.csv" into table `carriers` fields terminated by "," enclosed by '"' lines terminated by "\n";
load data infile "/tmp/sql/flights-small.csv" into table `flights` fields terminated by "," enclosed by '"' lines terminated by "\n";

-- QUESTION 1 Separator. DO NOT DELETE
select "QUESTION 1";
-- Put your Q1 SQL statements here
select distinct `flight_num` from `flights` where `origin_city` = "Seattle WA" and `dest_city` = "Boston MA" and `carrier_id` = (select `cid` from `carriers` where `name` = "Alaska Airlines Inc.") and `day_of_week_id` = (select `did` from `weekdays` where `day_of_week` = "Monday");

-- QUESTION 2 Separator. DO NOT DELETE
select "QUESTION 2";
-- Put your Q2 SQL statements here
select `day_of_week`, avg(`arrival_delay`) as `average_arrival_delay` from `flights`, `weekdays` where `flights`.`day_of_week_id` = `weekdays`.`did` group by `day_of_week` order by `average_arrival_delay` desc limit 3;

-- QUESTION 3 Separator. DO NOT DELETE
select "QUESTION 3";
-- Put your Q3 SQL statements here
select distinct `temp`.`name` as `airline_name` from (select `year`, `month_id`, `day_of_month`, `name`, count(*) as `number_of_flights` from `flights`, `carriers` where `flights`.`carrier_id` = `carriers`.`cid` group by `year`, `month_id`, `day_of_month`, `carrier_id`) `temp` where `temp`.`number_of_flights` > 1000;

-- QUESTION 4 Separator. DO NOT DELETE
select "QUESTION 4";
-- Put your Q4 SQL statements here
select `name` as `airline_name`, sum(`departure_delay`) as `total_departure_delay` from `flights`, `carriers` where `flights`.`carrier_id` = `carriers`.`cid` group by `name`;

-- QUESTION 5 Separator. DO NOT DELETE
select "QUESTION 5";
-- Put your Q5 SQL statements here
create temporary table `cancellation_count_table` select `name`, count(*) as `cancellation_count` from `flights`, `carriers` where `flights`.`cancelled` = 1 and `flights`.`origin_city` = "New York NY" and `flights`.`carrier_id` = `carriers`.`cid` group by `carrier_id`;
create temporary table `total_count_table` select `name`, count(*) as `total_count` from `flights`, `carriers` where `flights`.`origin_city` = "New York NY" and `flights`.`carrier_id` = `carriers`.`cid` group by `carrier_id`;
select `name` as `airline_name`, (`cancellation_count` / `total_count`) as `cancellation_percentage` from `cancellation_count_table` natural join `total_count_table` having `cancellation_percentage` > 0.005 order by `cancellation_percentage` asc;
