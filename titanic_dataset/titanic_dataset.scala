//Titanic Dataset - Spark

//Load Dataset
  val testDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("sandbox/data/test.csv")  // local file path

  val trainDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("sandbox/data/train.csv")  // local file path

//Merge Datasets Together
  val editedTrainDF = trainDF.drop("Survived")
  val completeDF = editedTrainDF.union(testDF)
  completeDF.show(false)

//train.csv Dataset
  //Number survived by gender
    trainDF.groupBy($"Sex")
      .sum("Survived")
      .withColumnRenamed("sum(Survived)", "NumSurvived")
      .show(false)

  //Survival in relation to family size
    //Create UDF to clean up formatting on names
    def clean_up: String => String = _.replaceAll('"'.toString, "")
    val udf_clean = udf(clean_up)

    //Clean up formatting on names
    val cleanNameDF = trainDF.withColumn("Name", udf_clean($"Name"))
    //Extract just 'surnames'
    val surnameDF = cleanNameDF.withColumn("Surname", split($"Name", ",").getItem(0)).withColumn("FamilySize", $"SibSp" + $"Parch" + 1)

    //Compile families together
    val famGroupedDF = surnameDF.groupBy($"Surname", $"Pclass", $"FamilySize", $"Embarked").sum("Survived").withColumnRenamed("sum(Survived)", "Survived")

    //Count number of families of that size
    val famTempDF1 = famGroupedDF.groupBy($"FamilySize").count.withColumnRenamed("count", "NumFam").orderBy("FamilySize")
    //Sum number of survivors for each size of family
    val famTempDF2 = famGroupedDF.groupBy($"FamilySize").sum("Survived").withColumnRenamed("sum(Survived)", "Survived")
    //Number of members in each family of that size
    val famTempDF3 = surnameDF.groupBy($"FamilySize").count.withColumnRenamed("count", "NumFamMembers")


    val famSurvivedDF = famTempDF1.join(famTempDF2, "FamilySize").join(famTempDF3, "Family").withColumn("Dead", $"NumFamMembers" - $"Survived")
    famSurvivedDF.show(false)


//train.csv and test.csv Datasets
  //Average ticket price by class //replace 'testDF' with 'trainDF' for other dataset
    val avgTempDF1 = testDF.groupBy("Pclass").sum("Fare").withColumnRenamed("sum(Fare)", "TotalFare")
    val avgTempDF2 = testDF.groupBy("Pclass").count.withColumnRenamed("count", "NumPass")
    val avgTempDF3 = tempDF1.join(tempDF2, "Pclass")
    val avgFareDF = tempDF3.withColumn("AvgFare", $"TotalFare" / $"NumPass")
    avgFareDF.orderBy("Pclass").show(false)

  //Averge ticket price today (adjusted for inflation)
    avgFareDF.withColumn("AvgFare2017", $"AvgFare" * 24.4238).orderBy("Pclass").show(false)

  //Passengers grouped by family size
    testDF.withColumn("Family", $"SibSp" + $"Parch" + 1).groupBy("Family")
      .count.orderBy("Family").show(false)

  //Number by gender
    trainDF.groupBy($"Sex").count.withColumnRenamed("count", "NumPass").show(false)

  //Number by gender and age range(10s)
    import org.apache.spark.ml.feature.Bucketizer

    val splits = (0 to 10).map(_ * 10.0).toArray
    val bucketizer = new Bucketizer()
      .setInputCol("Age")
      .setOutputCol("Bucket")
      .setSplits(splits)

    val bucketized = bucketizer.transform(completeDF)
    val temp1 = bucketized.groupBy($"Bucket", $"Sex").count.withColumnRenamed("count", "NumPass").orderBy($"Bucket")
    val temp2 = bucketized.groupBy($"Bucket", $"Sex").sum("Fare").withColumnRenamed("sum(Fare)", "TotalFare").orderBy($"Bucket")

    val temp3 = temp1.join(temp2, Seq("Bucket", "Sex"))

    val genAgeFareDF = temp3.withColumn("AgeRange", $"Bucket" * 10)
      .withColumn("AvgFare" $"TotalFare" / $"NumPass")
      .select("AgeRange", "Sex", "NumPass", "TotalFare", "AvgFare")
      .orderBy($"AgeRange", $"Sex")

    genAgeFareDF.show(false)
