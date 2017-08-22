/* Golden State Warriors Passing Network */

//import dependencies
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

object GSWPassNet {
  def main(args: Array[String]) {
    //initialize spark session
    va spark = SparkSession.builder.appName("GSW Passing Network Graph").getOrCreate()
    
    import spark.implicits._
    
    //load data
      val passes_data = spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(args(0))  // local file path

    //initalize 'id' values
      val edit_passes_data = passes_data.withColumn("id", split($"PLAYER", ",")
        .getItem(0)).withColumnRenamed("PLAYER", "name")
        .withColumnRenamed("PASS_TO", "dst")
        .withColumnRenamed("PASS", "count")

    //initalize vertices
      val v = edit_passes_data.select("id", "name").distinct

    //initalize edges
      val passes_raw = spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(args(1))  // local file path

      //'raw.csv' created using java
      val e = passes_raw
        .withColumn("src", split($"PLAYER", ",").getItem(0)).drop("PLAYER")
        .withColumn("dst", split($"PASS_TO", ",").getItem(0)).drop("PASS_TO")
        .drop("PASS")


    //initalize graphframe
      val g = GraphFrame(v, e)

      v.cache
      e.cache
      //run 'count' to persist in memory

    //run graphframe algorithims
    //UNDERSTAND WHAT EACH ALGORITHIM IS USED FOR AND HOW IT WORKS
        g.labelPropagation.maxIter(5).run.show(false)
        g.pageRank.resetProbability(0.15)
          .tol(0.01).run.show(false)
          //or
          .maxIter(10).run.show(false)
        g.shortestPaths.landmarks(Seq("Curry", "Thompson")).run.show(false)
        g.triangleCount.run.show(false)
        g.connectedComponents.run.show(false)
        g.stronglyConnectedComponents.maxIter(10).run.show(false)
  }
}
