package es.us

import es.us.linkage.{Distance, Linkage, LinkageModel}
import es.us.spark.mllib.clustering.validation.Indices
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

object MainClusterIndicesWithoutLinkageModelExecution {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName(s"ClusteringIndices")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //Set up the global variables
    var numCluster = 3
    var numPoints = 300
    var linkageStrategy = "avg"
    var pathToCheckPoint = "B:\\checkpoints"
    var pathToDatabase = s"data/C$numCluster-D20-I100"
    var delimiter = ","

    //Set up the limits of the algorithm
    var minimumCluster = 3
    var maximumCluster = 9
    var pathToResult = ""

    if (args.length > 2){
      numCluster = args.apply(0).toInt
      numPoints = args.apply(1).toInt
      linkageStrategy = args.apply(2)
      pathToCheckPoint = args.apply(3)
      pathToDatabase = args.apply(4)
      delimiter = args.apply(5)
      minimumCluster = args.apply(6).toInt
      maximumCluster = args.apply(7).toInt
      pathToResult = args.apply(8)
    }

    //Create an Array with each DataSet posibility
    var arguments = List(Array[String]())

    for (k <- minimumCluster to maximumCluster) {
      val auxList = Array[String](s"$k")
      arguments = auxList :: arguments
    }

    arguments = arguments.take(arguments.length - 1).reverse

    val result = for (data <- arguments) yield {
      numCluster = data.apply(0).toInt

      //Load data
      val dataRead = spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .option("delimiter", delimiter)
        .csv(pathToDatabase)
        .cache()

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

      //Rename the columns and generate a new DataFrame copy of the previous to be able to do the subsequent filtered out in the join
      val newColumnsNames = Seq("valueAux", "indexAux")
      val dataAuxRenamed = dataAux.toDF(newColumnsNames: _*)

      //Calculate the distance between all points
      val distances = dataAux.crossJoin(dataAuxRenamed)
        .filter(r => r.getLong(1) < r.getLong(3))
        .map{r =>
          //Depending on the method chosen one to perform the distance, the value of the same will change
          val dist = Utils.distEuclidean(r.getSeq[Double](0), r.getSeq[Double](2))

          //Return the result saving: (point 1, point 2, the distance between both)
          (r.getLong(1), r.getLong(3), dist)
        }

      //Save the distances between all points in a RDD[Distance]
      val distancesRDD = distances.rdd.map(_.toString().replace("(", "").replace(")", ""))
        .map(s => s.split(',').map(_.toFloat))
        .map { case x =>
          new Distance(x(0).toInt, x(1).toInt, x(2))
        }.filter(x => x.getIdW1 < x.getIdW2).repartition(16)

      spark.sparkContext.setCheckpointDir(pathToCheckPoint)

      val linkage = new Linkage(numCluster, linkageStrategy)
      val clustering = linkage.runAlgorithm(distancesRDD, numPoints)

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
      println("*** Points = " + numPoints + "***")
      println("*** Linkage Strategy = " + linkageStrategy + "***")
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

      (s"$numCluster-", silhouetteValues._3, dunnValues._3, BDValues._1, BDValues._2, BDValues._3, BDValues._4)

    }

    //Save the results
    val stringRdd = spark.sparkContext.parallelize(result)

    println("Saving the result ...")
    stringRdd.repartition(1)
      .map(_.toString().replace("(", "").replace(")", ""))
      .saveAsTextFile(s"$pathToResult-Results-All_Indices-" + Utils.whatTimeIsIt())
    println("Results saved!")

    spark.stop()

  }

}
