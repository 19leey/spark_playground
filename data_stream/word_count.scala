//Word Count using Netcat
import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import spark.implicits._

//socket stream
//read in a line from source
val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).option("includeTimestamp", true).load

//file stream - not implemented
//val lines = spark.readStream.option("header", "true").option("inferSchema", "true").csv("path...")

//split line into words
val words = lines.as[(String, Timestamp)].flatMap(line => line._1.split(" ").map(word => (word, line._2))).toDF("word", "timestamp")

//create dataframe of counts of words
val wordCounts = words.groupBy("word").count

//implement window operations
//val windowedCounts = words.groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"word").count

//initialize the query steam
val query = wordCounts.writeStream.outputMode("complete").format("console").option("truncate", "false").start
query.awaitTermination
