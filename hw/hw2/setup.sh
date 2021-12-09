#!/bin/sh
# Install Hadoop, Spark, and Zeppelin on the local machine
# Adapted from various multiple sources by James Raphael Tiovalen (2021)
# All rights reserved

# Install the necessary default package dependencies
sudo apt-get install default-jdk scala git -y

# TAR file link source: https://hadoop.apache.org/releases.html
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
tar -xzvf hadoop-3.3.1.tar.gz
sudo mv hadoop-3.3.1 /usr/local/hadoop
echo 'export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")' > /usr/local/hadoop/etc/hadoop/hadoop-env.sh
echo 'export PATH=$PATH:/usr/local/hadoop/bin' >> ~/.bashrc
source ~/.bashrc
hadoop version

# TAR file link source: https://spark.apache.org/downloads.html
wget https://dlcdn.apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz
tar -xzvf spark-3.2.0-bin-hadoop3.2.tgz
sudo mv spark-3.2.0-bin-hadoop3.2 /usr/local/spark
echo 'export PATH=$PATH:/usr/local/spark/bin' >> ~/.bashrc
source ~/.bashrc
spark-submit --version

# TAR file link source: https://zeppelin.apache.org/download.html
wget https://dlcdn.apache.org/zeppelin/zeppelin-0.10.0/zeppelin-0.10.0-bin-all.tgz
tar -xzvf zeppelin-0.10.0-bin-all.tgz
sudo mv zeppelin-0.10.0-bin-all /usr/local/zeppelin
echo -e "
export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
export HADOOP_HOME=/usr/local/hadoop
export SPARK_HOME=/usr/local/spark
export HADOOP_CONF_DIR=\${HADOOP_HOME}/etc/hadoop
export PYSPARK_PYTHON=python3
export MASTER=yarn-client
" >> /usr/local/zeppelin/conf/zeppelin-env.sh
cp /usr/local/zeppelin/conf/zeppelin-site.xml.template /usr/local/zeppelin/conf/zeppelin-site.xml
sed -i "s/<value>127.0.0.1<\/value>/<value>0.0.0.0<\/value>/g" /usr/local/zeppelin/conf/zeppelin-site.xml
sed -i "s/<value>8080<\/value>/<value>9090<\/value>/g" /usr/local/zeppelin/conf/zeppelin-site.xml
sed -i "s/<value>8443<\/value>/<value>9443<\/value>/g" /usr/local/zeppelin/conf/zeppelin-site.xml
source ~/.bashrc

# Remove all downloaded files
rm -rf hadoop-3.3.1.tar.gz spark-3.2.0-bin-hadoop3.2.tgz zeppelin-0.10.0-bin-all.tgz

# Other auxiliary command stuff to set up the local environment and start up all of the necessary services
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
cp core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml

# Start up the services (need to do this every time you start up the environment after system reboots)
hadoop namenode -format
/usr/local/hadoop/sbin/start-dfs.sh && /usr/local/hadoop/sbin/start-yarn.sh
/usr/local/spark/sbin/start-all.sh
/usr/local/zeppelin/bin/zeppelin-daemon.sh start
