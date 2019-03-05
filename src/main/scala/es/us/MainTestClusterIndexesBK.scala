package es.us

import org.apache.spark.mllib.clustering.BisectingKMeans
import es.us.spark.mllib.clustering.validation.Indexes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

object MainTestClusterIndexesBK {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName(s"VariablesIndexes")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //Set up the global variables
    var numVariables = 0
    var numCluster = 0
    var numPoints = 0
    var dataFile = ""

    val delimiter = ","

    //Set up the limits of the algorithm
    val minimumCluster = 3
    val maximumCluster = 9
    val minimumVariable = 1
    val maximumVariable = 20
    val limitVariables = 19

    val listK = List(300,500,700)

    //Create an Array with each DataSet possibility
    var arguments = List(Array[String]())

    for (kvalue <- listK) {
      for (nv <- minimumVariable to maximumVariable) {
        for (k <- minimumCluster to maximumCluster) {
          val auxList = Array[String](s"$nv", s"$k", s"$kvalue")
          arguments = auxList :: arguments
        }
      }
    }

    arguments = arguments.take(arguments.length - 1).reverse

    val result = for (data <- arguments) yield {
      numCluster = data.apply(1).toInt
      numVariables = data.apply(0).toInt
      numPoints = data.apply(2).toInt

      val clusterPerPoints = (numPoints/100)

      dataFile = s"data/C$clusterPerPoints-D20-I100"

      //Load data
      var dataRead = spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .option("delimiter", delimiter)
        .csv(dataFile)
        .cache()

      //Delete columns to the DataSet depending on the number of variables
      for (i <- numVariables to limitVariables){
        val index = i
        dataRead = dataRead.drop(s"_c$index")
      }

      //Save all columns as Seq[Double]
      val dataReadSeq = dataRead.map(_.toSeq.asInstanceOf[Seq[Double]])

      //Create a RDD with each points in Vector format
      val dataRDD = dataReadSeq.rdd.map { r =>

        //Create a Vector with the Array[Vector] of each row in the DataSet read
        val auxVector = Vectors.dense(r.toArray)

        (auxVector)
      }

      val startClustering = System.nanoTime

      //Create BisectingKMeansModel
      val bk_means = new BisectingKMeans().setK(numCluster)
      val clustering = bk_means.run(dataRDD)

      val durationClustering = (System.nanoTime - startClustering) / 1e9d

      //Create a RDD with each cluster and they points
      val dataClusterPoints = dataReadSeq.rdd.map { r =>

        //Create a Vector with the Array[Vector] of each row in the DataSet read
        val auxVector = Vectors.dense(r.toArray)

        //Return the Cluster ID and the Vector for each row in the DataSet read
        (clustering.predict(auxVector).hashCode, auxVector)
      }.groupByKey()

      val start = System.nanoTime

      println("*** K = " + numCluster + " ***")
      println("*** NV = " + numVariables + "***")
      println("*** Points = " + numPoints + "***")
      println("Executing Indices ...")
      val silhouetteValues = Indexes.getSilhouette(dataClusterPoints.collect())
      val dunnValues = Indexes.getDunn(dataClusterPoints.collect())
      val BDValues = Indexes.getIndexesBD_BKM(clustering, dataRDD, numCluster, spark.sparkContext)
      println("VALUES:")
      println("\tSilhouette: " + silhouetteValues._3)
      println("\tDunn: " + dunnValues._3)
      println("\tSilhouette-BD: " + BDValues._1)
      println("\tDunn-BD: " + BDValues._2)
      println("\tDavis-Bouldin: " + BDValues._3)
      println("\tWSSE: " + BDValues._4)
      println("\n")

      val duration = (System.nanoTime - start) / 1e9d

      (s"$numPoints-$numVariables-$numCluster", silhouetteValues._3, dunnValues._3, BDValues._1, BDValues._2, BDValues._3, BDValues._4, silhouetteValues._4, dunnValues._4, BDValues._5, BDValues._6, BDValues._7, duration, durationClustering)

    }

    //Save the results
    val stringRdd = spark.sparkContext.parallelize(result)

    println("Saving the result ...")
    stringRdd.repartition(1)
      .map(_.toString().replace("(", "").replace(")", ""))
      .saveAsTextFile(s"-Results-All_Indexes-" + Utils.whatTimeIsIt())
    println("Results saved!")

    spark.stop()

  }

}
