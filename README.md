# Spark Playground
## spark programs to test out different libraries and functions

### Titanic Dataset
* Exposure to Spark's Dataset/DataFrame API
* Load 'csv' files and manipulate the data
  * Manipulations done using Spark SQL functions
  
### Golden State Warriors: Passing Network
* Testing out Spark's GraphX/GraphFrame API
* Generate and analyze a network graph of each player's passes
* Heavily based on **[this](http://opiateforthemass.es/articles/analyzing-golden-state-warriors-passing-network-using-graphframes-in-spark/)** project

### Iris Machine Learning
* Use MLlib/ML Spark library
* Train program to catagorize iris measurments to species

### Building Security
* Generate a graph of a building's layout
* Given input of a person's path, detect anomalies
	* Use of motif finding

### Market Trend Predictor
* Implementation of the Spark-TS library
* Predict market trends based on S&P 500, DOW, and NASDAQ
	* Historical data from Yahoo Finance (1985-2017)
* Time series forecasting using the ARIMA model
	* Inaccurate - too many variables unaccounted for
	
### Data Streaming

#### Installed dependencies:
~~~~
scala 2.12.2
OpenJDK 64-Bit Java 1.8.0_131
spark 2.2.0
sbt 0.13.15
graphframes 0.5.0
spark-ts 0.4.0
kafka 0.11.0.0
zookeeper // should come with kafka
~~~~
