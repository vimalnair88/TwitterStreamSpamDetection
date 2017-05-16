# Twitter Stream Spam Detection

An academic project for detecting Spam in Stream of Twitter data, developed using Apache Spark, Apache Kafka, Zookeeper and Tweepy Python module.

# Developers

1. George Zachariah
2. Vimal Nair

# How to run

Step 1: Start the Zookeeper on the server

zookeeper-server-start.bat C:\kafka_2.11-0.9.0.0\kafka_2.11-0.9.0.0\config\zookeeper.properties

Step 2: Start the Kafka Server.

kafka-server-start.bat C:\kafka_2.11-0.9.0.0\kafka_2.11-0.9.0.0\config\server.properties

Step 3: Create a topic for the Twitter Stream

kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitterstream

Step 4: Run the python script

python PythonProducer\app.py

Step 5: Run the KafkaExample.scala
