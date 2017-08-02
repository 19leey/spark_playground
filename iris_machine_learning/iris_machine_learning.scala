//Iris Dataset

//Import required
  import org.apache.spark.ml._
  import org.apache.spark.ml.feature._
  import org.apache.spark.ml.classification._
  import org.apache.spark.ml.evaluation._
  import org.apache.spark.ml.linalg.Vectors
  import org.apache.spark.ml.Pipeline

//Load dataset
  val iris = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Downloads/Iris.csv")
  iris.show(false)

//Edit data set - features vector and type column
  val assembeler = new VectorAssembler().setInputCol(Array("SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm")).setOutputCol("features")
  val iris_data = assembeler.transform(iris).withColumnRenamed("Species", "type").select("features", "type")


//Set up indexers
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
