/* Iris Dataset Machine Learning */

//import dependencies
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.ml._
  import org.apache.spark.ml.feature._
  import org.apache.spark.ml.classification._
  import org.apache.spark.ml.evaluation._
  import org.apache.spark.ml.linalg.Vectors
  import org.apache.spark.ml.Pipeline

object IrisML {
  def main(args: Array[String]) {
    //initialize spark session
    val spark = SparkSession.builder.appName("Iris Data Machine Learning").getOrCreate()
    
    import spark.implicits._
    
    //load dataset
      val iris = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load(args(0))
      iris.show(false)

    //edit data set - features vector and type column
      val assembeler = new VectorAssembler().setInputCols(Array("SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm")).setOutputCol("features")
      val iris_data = assembeler.transform(iris).withColumnRenamed("Species", "type").select("features", "type")


    //set up indexers
      val labelIndexer = new StringIndexer()
        .setInputCol("type")
        .setOutputCol("indexedLabel")
        .fit(iris_data)

      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4)
        .fit(iris_data)


    //LogisticRegression - ONLY works for numerical values(not catagorical aka. strings)

    spark.stop()
  }
}
