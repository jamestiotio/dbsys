create table weather (
date date not null,
max_temp real,
mean_temp real,
min_temp real,
max_visibility_miles real,
mean_visibility_miles real,
min_visibility_miles real,
max_wind_speed_mph real,
mean_wind_speed_mph real,
max_gust_speed_mph real,
cloud_cover real,
events text,
wind_dir_degrees real,
zip_code varchar(5) not null,
PRIMARY KEY (date, zip_code)
);
create index zip_code_idx on weather (zip_code);




create table station (
station_id smallint not null, 
station_name text,
latitude real,
longitude real,
dock_count smallint,
city text,
installation_date date,
zip_code varchar(5),
PRIMARY KEY (station_id),
FOREIGN KEY (zip_code) REFERENCES weather(zip_code)
);



create table trip (
id integer not null,
duration integer,
start_time timestamp,
start_station_name text,
start_station_id smallint,
end_time timestamp,
end_station_name text,
end_station_id smallint,
bike_id smallint,
PRIMARY KEY(id),
FOREIGN KEY(start_station_id) REFERENCES station(station_id),
FOREIGN KEY(end_station_id) REFERENCES station(station_id)
);

-- Q1
select count(distinct city) from station;

-- Q2
select city, count(distinct station_id) as station_count from station group by city;

-- Q3
select count(*) from trip where start_station_id = end_station_id;

-- Q4
create temporary table self_loop_trip_count select count(*) as c from trip where start_station_id = end_station_id;
create temporary table total_trip_count select count(*) as c from trip;
select s.c / t.c from self_loop_trip_count s, total_trip_count t;

-- alt Q4 answer

select sum(start_station_id = end_station_id)/count(*) as ratio from trip;


-- Q5
select ss.city, count(t.id) as c from trip t, station ss, station es where t.start_station_id = ss.station_id and t.end_station_id = es.station_id and ss.city = es.city group by ss.city order by c desc limit 1;

-- Q6

create temporary table bike_with_start_city select t.bike_id, s.city from trip t, station s where t.start_station_id = s.station_id;
create temporary table bike_with_end_city select t.bike_id, s.city from trip t, station s where t.end_station_id = s.station_id;
create temporary table bike_with_city select * from bike_with_start_city union select * from bike_with_end_city;

select bike_id, count(distinct city) as city_count from bike_with_city group by bike_id having city_count > 1;

-- Q7

create temporary table bike_with_start_station select bike_id, start_station_name as station_name from trip; 
create temporary table bike_with_end_station select bike_id, end_station_name as station_name from trip; 
create temporary table bike_with_station select * from bike_with_start_station union select * from bike_with_end_station;
select distinct bike_id from trip where bike_id not in (select bike_id from bike_with_station where station_name = 'Japantown');

