//Builidng Graphframe Analysis

//Import
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.graphframes._
  import scala.collection.JavaConverters._
  import java.sql._

//Load data
  val rf1 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/building_data/rooms_f1_data.csv")
  val df1 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/building_data/doors_f1_data.csv")
  val rf2 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/building_data/rooms_f2_data.csv")
  val df2 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/building_data/doors_f2_data.csv")
  val rf3 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/building_data/rooms_f3_data.csv")
  val df3 = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/building_data/doors_f3_data.csv")

  val floors = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/building_data/floors_data.csv")
  val stairs = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/building_data/stairs_data.csv")

  val rooms = rf1.union(rf2).union(rf3)
  val doors = df1.union(df2).union(df3)

  //Create verticies and edges for graph
  val v = rooms.union(floors)
  val e = doors.union(stairs)

//Create graphframe
  val g = GraphFrame(rooms, doors)

//Load log data
  val valid_log = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/building_data/valid_log_data.csv")
  val invalid_log = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/building_data/invalid_log_data.csv")

//List of possible paths given a starting location (motif finding)
  g.find("(a)-[]->(b)").filter("a.name = 'Office 1'").show(false)
    .filter($"a.name" === "Office 1") //alternate syntax
    //(a)-[]->(b) - specifies the edges/vertices in question, can link on more conditions using ';'
    //filter condition filters out just the paths originating at 'Office 1' - swap conditions for origin/end

//Function to see if location is a floor
  def isFloor (row: Row) : Boolean = {
    return (row.getString(0).charAt(0) == 'F')
  }

//Function to see if floor location is valid
  def validFloorLocation (row: Row, prev: Row, next: Row) : Boolean = {
    //store origin floor and end floor
    var floor_origin = log.getString(0).charAt(1).asDigit
    var floor_end = log.getString(1).charAt(1).asDigit

    //store previous floor and next floor
    var floor_prev = prev.getString(1).charAt(1).asDigit
    var floor_next = prev.getString(0).charAt(1).asDigit

    return (floor_origin == floor_prev && floor_end == floor_next)
  }

//Function to check if starting location is valid based on last logged location
  def vaildLocation (path: DataFrame) : Boolean = {
    //define iterator
    val itr = path.toLocalIterator.asScala
    //store previous location
    var prev = itr.next

    //iterator through log
    while (itr.hasNext) {
      //store current location
      var curr = itr.next
      //check if room location is not a match and current and previous locations are not floors
      if (prev(1) != curr(0) && !isFloor(prev) && !isFloor(curr)) {
        println("Room location is not a match >> " + prev(1) + " to " + curr(0))
        return false
      }
      //check if current location is a floor
      else if(isFloor(curr)) {
        //store value of next row
        var next = itr.next

        //floor location is not a match
        if(!validFloorLocation(curr, prev, next)) {
          println("Floor location is not a match")
          return false
        }
        //update previous location
        else {
          prev = next
        }
      }
      else {
        //update previous location
        prev = curr
      }
    }
    //all locations were a match
    println("Locations all match up")
    return true
  }

//Function to cast timestamp column to type timestamp
  def castTimestamp (df: DataFrame) : DataFrame = {
    //redo timestamp column
    val log_ts = df.withColumn("temp", to_timestamp($"time", "MM/dd/yyyy HH:mm:ss"))
      .drop("time").withColumnRenamed("temp", "time")

    return log_ts
  }

//Function to calculate time difference in milliseconds
  def timeDifference (subtractee : Row, subtractor : Row) : Long = {
    return (subtractee.getTimestamp(2).getTime - subtractor.getTimestamp(2).getTime)
  }

//Function to check if timestamp is valid
  def validTimestamp (log: DataFrame) : Boolean = {
    //define an iterator
    val itr = castTimestamp(log).toLocalIterator.asScala
    //store previous row
    var prev = itr.next

    //iterate through dataframe
    while (itr.hasNext) {
      //store current row
      var curr = itr.next

      //check if time is going backwards
      if (timeDifference(curr, prev) < 0) {
        println("Time Traveling >> " + prev.get(0) + " to " + curr.get(0))
        return false
      }
      //check if time is too short (moving too fast)
      else if (timeDifference(curr, prev) < 30000) {
        println("Moivng too fast >> " + prev.get(0) + " to " + curr.get(0))
        return false
      }
      //check if time is too long (inactive for too long)
      else if (timeDifference(curr, prev) > 259200000) {
        println("You died inside >> " + prev.get(0))
        return false
      }
      //update previous
      prev = curr
    }
    println("Timestamps valid")
    return true
  }

//Function to take path and graphframe as input, and output whether path is valid (possible)
  def validPath (graph: GraphFrame, path: DataFrame) = {
    //only proceed after checking that locations match up
    if (!validLocation(path) || !validTimestamp(path)) {
      println("Anomoly Detected")
    }
    else {
      //define an iterator
      val itr = path.toLocalIterator.asScala

      //iterate through path log
      while (itr.hasNext) {
        //store the row (current path segment)
        var row = itr.next

        //check if path exists
        //path does not exist
        if(graph.find("(a)-[]->(b)").filter($"a.id" === row(0) && $"b.id" === row(1)).count == 0) {
          print("Anomoly Detected")
        }
        //path exists
        else {
          print("Valid Route")
        }
        println(" >> " + row(0) + " to " + row(1))
      }
    }
  }
