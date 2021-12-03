
# change the following according to your student numbers
echo "1001234,1003456"

# change the following according to your environment
hdfs_namenode="localhost"

# don't change following

echo "part1"
hdfs dfs -rm -r /assignment2/part1/
hdfs dfs -mkdir -p /assignment2/part1/input/
hdfs dfs -put ./data/TA_restaurants_curated_cleaned.csv /assignment2/part1/input/TA_restaurants_curated_cleaned.csv

echo "question 1"
hdfs dfs -rm -r /assignment2/output/question1
spark-submit q1.py $hdfs_namenode 

echo "question 2" 
hdfs dfs -rm -r /assignment2/output/question2
spark-submit q2.py $hdfs_namenode 

echo "question 3" 
hdfs dfs -rm -r /assignment2/output/question3
spark-submit q3.py $hdfs_namenode 

echo "question 4"
hdfs dfs -rm -r /assignment2/output/question4
spark-submit q4.py $hdfs_namenode 

echo "part2"
hdfs dfs -rm -r /assignment2/part2/
hdfs dfs -mkdir -p /assignment2/part2/input/
hdfs dfs -put ./data/tmdb_5000_credits.parquet /assignment2/part2/input/tmdb_5000_credits.parquet


echo "question 5"
hdfs dfs -rm -r /assignment2/output/question5
spark-submit q5.py $hdfs_namenode 


