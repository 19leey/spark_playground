// import dependencies
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.cloudera.sparkts._
import com.cloudera.sparkts.models._
import java.time._

// read in the raw historical data
// S&P 500, Dow Jones 30, and NASDAQ
val sp_raw_data = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/predict/SP_500.csv")
val dj_raw_data = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/predict/DJ_30.csv")
val nasdaq_raw_data = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("Desktop/predict/NASDAQ.csv")

// format data for merging(union)
// add ticker symbol to differentiate
val sp_data = sp_raw_data.select("Date", "Adj Close").withColumnRenamed("Adj Close", "Close").withColumn("Ticker", typedLit("INX"))
val dj_data = dj_raw_data.select("Date", "Adj Close").withColumnRenamed("Adj Close", "Close").withColumn("Ticker", typedLit("DJI"))
val nasdaq_data = nasdaq_raw_data.select("Date", "Adj Close").withColumnRenamed("Adj Close", "Close").withColumn("Ticker", typedLit("IXIC"))

// union data
val data = sp_data.union(dj_data).union(nasdaq_data).withColumnRenamed("Date", "Timestamp")

// sort data
val tsdf = data.select("Timestamp", "Ticker", "Close").orderBy($"Timestamp".asc)

// cache data
tsdf.registerTempTable("InitialData")
tsdf.count

// get the range of the timestamps
val minDate = tsdf.selectExpr("min(Timestamp)").collect()(0).getTimestamp(0)
val maxDate = tsdf.selectExpr("max(Timestamp)").collect()(0).getTimestamp(0)

// get local time zone
val zone = ZoneId.systemDefault()

// generate a date time index using timestamp range and local time zone
val dtIndex = DateTimeIndex.uniformFromInterval(ZonedDateTime.of(minDate.toLocalDateTime, zone),ZonedDateTime.of(maxDate.toLocalDateTime, zone),new DayFrequency(1))

// generate a timeseriesRDD from the data
val tsRDD = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, tsdf, "Timestamp", "Ticker", "Close")


//val df = tsRDD.mapSeries{vector => {
//val newVec = new org.apache.spark.mllib.linalg.DenseVector(vector.toArray.map(x => if(x.equals(Double.NaN)) 0 else x))
//val arimaModel = ARIMA.fitModel(1, 0, 0, newVec)
//val forecasted = arimaModel.forecast(newVec, DAYS)
//new org.apache.spark.mllib.linalg.DenseVector(forecasted.toArray.slice(forecasted.size-(DAYS+1), forecasted.size-1))
//}}.toDF("symbol","values")


//spark-shell --jars spark-ts/spark-timeseries-master/target/sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar -i Desktop/predict/predict_trends.scala
