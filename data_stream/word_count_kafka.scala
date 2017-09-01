//Word Count using Kafka
//Setting Up Kafka
//  start zookeeper
//    ./bin/zookeeper-server-start.sh config/zookeeper.properties

//  start kafka
//   ./bin/kafka-server-start.sh config/server.properties

//  create kafka topic
//  name is arbitrary - [spark-topic]
//    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic [name]
//  list of kafka topics
//    ./bin/kafka-topics.sh --list --zookeeper localhost:2181

//Sending and Receiving Messages - Kafka
//  send messages - produce
//    ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic [name]
//  receive messages - consume
//  define how you want to recieve messages
//    ./bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic [name] --from-beginning


//Setting up Spark Streaming
//Streaming
//  import org.apache.spark.streaming.kafka._

//  val ssc = new StreamingContext(sc, Seconds(10))
//  val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming-consumer-group", Map("spark-topic" -> 5))

//Structured Streaming
//import dependencies
import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.streaming._

//read data from kafka producer
//from file
//  define new file input topic (optional)
//  load file into producer
//    "./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-file-input < [file input path]"
val kafkaStream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "spark-topic").load()
//seperate kafka producer data into words and count distinct words
//manipulate the source data
val words = kafkaStream.select(explode(split($"value".cast("string"), "\\s+")).as("word")).groupBy("word").count

//start query to output word count to console
val query = words.writeStream.outputMode("complete").format("console").option("truncate", "false").start()
query.awaitTermination()


//run
//spark-shell --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 -i Desktop/data_stream/[filename].scala
//spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 -i Desktop/data_stream/[filename].scala
