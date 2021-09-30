#!/bin/bash
# Created by James Raphael Tiovalen (2021)
# We assume that we use MySQL v8.0

# Print/display all table creation statements
sed -n -e '/CREATE TABLE/,/;/ p' setup_v8.sql

# Setup database
mysql -u root -p -e "DROP DATABASE IF EXISTS \`bike_sharing\`; CREATE DATABASE \`bike_sharing\`;"
mysql -u root -p bike_sharing <setup_v8.sql

# Q1: Count the number of cities (no duplicates).
mysql -u root -p bike_sharing -e "SELECT COUNT(DISTINCT \`city\`) FROM \`station\`;"

# Q2: Count the number of stations in each city. Output the city name and station count.
mysql -u root -p bike_sharing -e "SELECT \`city\`, COUNT(\`station_id\`) FROM \`station\` GROUP BY \`city\`;"

# Q3: Count the number of self-loop trips. A self-loop trip is one that starts and ends at the same station.
mysql -u root -p bike_sharing -e "SELECT COUNT(\`id\`) FROM \`trip\` WHERE \`trip\`.\`start_station_id\` = \`trip\`.\`end_station_id\`;"

# Q4: Print the ratio of self-loop trips over all trips.
mysql -u root -p bike_sharing -e "SELECT (SELECT COUNT(\`id\`) FROM \`trip\` WHERE \`trip\`.\`start_station_id\` = \`trip\`.\`end_station_id\`) / (SELECT COUNT(\`id\`) FROM \`trip\`) AS \`ratio\`;"

# Q5: Find the most popular city, in terms of percentage of trips belonging to the city.
mysql -u root -p bike_sharing -e "SELECT \`ss\`.\`city\`, COUNT(\`t\`.\`id\`) AS \`c\` FROM \`trip\` \`t\`, \`station\` \`ss\`, \`station\` \`es\` WHERE \`t\`.\`start_station_id\` = \`ss\`.\`station_id\` AND \`t\`.\`end_station_id\` = \`es\`.\`station_id\` AND \`ss\`.\`city\` = \`es\`.\`city\` GROUP BY \`ss\`.\`city\` ORDER BY \`c\` DESC LIMIT 1;"

# Q6: Find all their bikes (their `bike_id`) that have been to more than 1 city. A bike has been to a city if its start or end station of one of its trips is in this city.
mysql -u root -p bike_sharing -e "CREATE TEMPORARY TABLE \`bike_with_start_city\` SELECT \`t\`.\`bike_id\`, \`s\`.\`city\`FROM \`trip\` \`t\`, \`station\` \`s\` WHERE \`t\`.\`start_station_id\` = \`s\`.\`station_id\`; CREATE TEMPORARY TABLE \`bike_with_end_city\` SELECT \`t\`.\`bike_id\`, \`s\`.\`city\` FROM \`trip\` \`t\`, \`station\` \`s\` WHERE \`t\`.\`end_station_id\` = \`s\`.\`station_id\`; CREATE TEMPORARY TABLE \`bike_with_city\` SELECT * FROM \`bike_with_start_city\` UNION SELECT * FROM \`bike_with_end_city\`; SELECT \`bike_id\`, COUNT(\`city\`) AS \`city_count\` FROM \`bike_with_city\` GROUP BY \`bike_id\` HAVING \`city_count\` > 1;"

# Q7: List the bikes (their `bike_id`) that have never been to Japantown station.
mysql -u root -p bike_sharing -e "CREATE TEMPORARY TABLE \`bike_id_with_start_station_id\` SELECT \`t\`.\`bike_id\`, \`s\`.\`station_id\` FROM \`trip\` \`t\`, \`station\` \`s\` WHERE \`t\`.\`start_station_id\` = \`s\`.\`station_id\`; CREATE TEMPORARY TABLE \`bike_id_with_end_station_id\` SELECT \`t\`.\`bike_id\`, \`s\`.\`station_id\` FROM \`trip\` \`t\`, \`station\` \`s\` WHERE \`t\`.\`end_station_id\` = \`s\`.\`station_id\`; CREATE TEMPORARY TABLE \`bike_id_with_station_id_1\` SELECT * FROM \`bike_id_with_start_station_id\` UNION SELECT * FROM \`bike_id_with_end_station_id\`; CREATE TEMPORARY TABLE \`bike_id_with_station_id_2\` SELECT * FROM \`bike_id_with_start_station_id\` UNION SELECT * FROM \`bike_id_with_end_station_id\`; SELECT DISTINCT \`bike_id\` FROM \`bike_id_with_station_id_1\` WHERE \`bike_id\` NOT IN (SELECT \`bike_id\` FROM \`bike_id_with_station_id_2\` WHERE \`station_id\` = (SELECT \`station_id\` FROM \`station\` WHERE \`station_name\` = 'Japantown'));"
