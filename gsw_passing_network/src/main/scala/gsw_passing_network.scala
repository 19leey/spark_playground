//Golden State Warriors Passing Netowrk

//Import requried
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

//Load data
  val passes_data = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("Downloads/gsw_passing_network/passes.csv")  // local file path

//Initalize 'id' values
  val edit_passes_data = passes_data.withColumn("id", split($"PLAYER", ",")
    .getItem(0)).withColumnRenamed("PLAYER", "name")
    .withColumnRenamed("PASS_TO", "dst")
    .withColumnRenamed("PASS", "count")

//Initalize vertices
  val v = edit_passes_data.select("id", "name").distinct

//Initalize edges
  val passes_raw = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("Downloads/raw.csv")  // local file path

    //'raw.csv' created using java
    /*
    import java.io.*;

    public class Sandbox {

    	public static void main(String[] args) {
    		try {
    			File file = new File("passes.csv");
    			FileReader fr = new FileReader(file);
    			BufferedReader br = new BufferedReader(fr);
    			PrintWriter pw = new PrintWriter(new FileWriter("out.txt"));

    			String header = br.readLine();
    			System.out.println(header);
    			pw.write(header);
    			pw.write("\n");
    			String line;
    			while ((line = br.readLine()) != null) {
    				String[] word = line.split(",");
    				int passes = Integer.parseInt(word[4]);
    				for(int i = 0; i < passes; i++) {
    					System.out.println(line);
    					pw.write(line);
    					pw.write("\n");
    				}
    			}
    			fr.close();
    			pw.close();
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
    	}
    }
    */

  val e = raw
    .withColumn("src", split($"PLAYER", ",").getItem(0)).drop("PLAYER")
    .withColumn("dst", split($"PASS_TO", ",").getItem(0)).drop("PASS_TO")
    .drop("PASS")


//Initalize graphframe
  val g = GraphFrame(v, e)

  v.cache
  e.cache
  //Run 'count' to persist in memory

//Run Graphframe algorithims
@TODO
//UNDERSTAND WHAT EACH ALGORITHIM IS USED FOR AND HOW IT WORKS
    g.labelPropagation.maxIter(5).run.show(false)
    g.pageRank.resetProbability(0.15)
      .tol(0.01).run.show(false)
      //or
      .maxIter(10).run.show(false)
    g.shortestPaths.landmarks(Seq("Curry", "Thompson")).run.show(false)
    g.triangleCount.run.show(false)
    g.connectedComponents.run.show(false)
    g.stronglyConnectedComponents.maxIter(10).run.show(FamilySize)
