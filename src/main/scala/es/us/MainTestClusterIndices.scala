package es.us

import es.us.linkage.LinkageModel
import es.us.spark.mllib.clustering.validation.Indices
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

object MainTestClusterIndices {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName(s"VariablesIndices")
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

    //Create an Array with each DataSet posibility
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

      //Generate automatically an index for each row
      val dataAux = dataReadSeq.withColumn("index", monotonically_increasing_id()+1)

      //Save dataAux for futures uses
      val coordinates = dataAux.map(row => (row.getLong(1), row.getSeq[Double](0).toList))
        .rdd
        .sortByKey()
        .map(_.toString().replace("(", "").replace("))", ")").replace("List", "(").replace(",(", ";("))

      //Save the id and the coordinates of all points in a RDD[(Int,Vector)]
      val coordinatesRDD = coordinates
        .map(s => s.split(";"))
        .map(row => (row(0).toInt, Vectors.dense(row(1).replace("(", "").replace(")", "").split(",").map(_.toDouble))))

      spark.sparkContext.setCheckpointDir("B:\\checkpoints")

      val linkage = spark.sparkContext.textFile(s"linkageModels/Linkage-C$clusterPerPoints-$numPoints"+s"p-D$numVariables\\part-00000")
        .map(s => s.split(',').map(_.toInt))
        .map{
        case x => (x(0).toLong, (x(1), x(2)))}

      val clustering = new LinkageModel(linkage, spark.sparkContext.emptyRDD[Vector].collect())

      val totalPoints = spark.sparkContext.parallelize(1 to numPoints).cache()

      val centroids = clustering.inicializeCenters(coordinatesRDD, 1, numPoints, numCluster, totalPoints, numCluster)
      clustering.setClusterCenters(centroids)

      //Create a RDD with each cluster and they points
      val dataRDD = dataReadSeq.rdd.map { r =>

        //Create a Vector with the Array[Vector] of each row in the DataSet read
        val auxVector = Vectors.dense(r.toArray)

        //Return the Cluster ID and the Vector for each row in the DataSet read
        (clustering.predict(auxVector).hashCode, auxVector)
      }.groupByKey()

      println("*** K = " + numCluster + " ***")
      println("*** NV = " + numVariables + "***")
      println("*** Points = " + numPoints + "***")
      println("Executing Indices ...")
      val silhouetteValues = Indices.getSilhouette(dataRDD.collect())
      val dunnValues = Indices.getDunn(dataRDD.collect())
      val BDValues = Indices.getIndicesBD(clustering, spark.sparkContext, dataReadSeq, numCluster)
      println("VALUES:")
      println("\tSilhouette: " + silhouetteValues._3)
      println("\tDunn: " + dunnValues._3)
      println("\tSilhouette-BD: " + BDValues._1)
      println("\tDunn-BD: " + BDValues._2)
      println("\tDavid-Bouldin: " + BDValues._3)
      println("\tWSSE: " + BDValues._4)
      println("\n")

      (s"$numPoints-$numVariables-$numCluster", silhouetteValues._3, dunnValues._3, BDValues._1, BDValues._2, BDValues._3, BDValues._4)

    }

      //Save the results
      val stringRdd = spark.sparkContext.parallelize(result)

      println("Saving the result ...")
      stringRdd.repartition(1)
        .map(_.toString().replace("(", "").replace(")", ""))
        .saveAsTextFile(s"-Results-All_Indices-" + Utils.whatTimeIsIt())
      println("Results saved!")

    spark.stop()

  }

}
