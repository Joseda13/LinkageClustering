package es.us

import es.us.linkage.LinkageModel
import es.us.spark.mllib.clustering.validation.Indexes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

object MainClusterIndexesWithLinkageModelExecution {

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
    var pathToLinkageModel = s"linkageModels/Linkage-C3-$numPoints"+"p\\part-00000"
    var pathToCheckPoint = "B:\\checkpoints"
    var pathToDatabase = s"data/C$numCluster-D20-I100"
    var delimiter = ","

    //Set up the limits of the algorithm
    var minimumCluster = 2
    var maximumCluster = 10
    var pathToResult = ""

    if (args.length > 2){
      numCluster = args.apply(0).toInt
      numPoints = args.apply(1).toInt
      pathToLinkageModel = args.apply(2)
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

      spark.sparkContext.setCheckpointDir(pathToCheckPoint)

      val linkage = spark.sparkContext.textFile(pathToLinkageModel)
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
      println("*** Points = " + numPoints + "***")
      println("Executing Indices ...")
      val silhouetteValues = Indexes.getSilhouette(dataRDD.collect())
      val dunnValues = Indexes.getDunn(dataRDD.collect())
      val BDValues = Indexes.getIndexesBD_Linkage(clustering, spark.sparkContext, dataReadSeq, numCluster)
      println("VALUES:")
      println("\tSilhouette: " + silhouetteValues._3)
      println("\tDunn: " + dunnValues._3)
      println("\tSilhouette-BD: " + BDValues._1)
      println("\tDunn-BD: " + BDValues._2)
      println("\tDavis-Bouldin: " + BDValues._3)
      println("\n")

      (s"$numCluster-", silhouetteValues._3, dunnValues._3, BDValues._1, BDValues._2, BDValues._3)

    }

    //Save the results
    val stringRdd = spark.sparkContext.parallelize(result)

    println("Saving the result ...")
    stringRdd.repartition(1)
      .map(_.toString().replace("(", "").replace(")", ""))
      .saveAsTextFile(s"$pathToResult-Results-All_Indexes-" + Utils.whatTimeIsIt())
    println("Results saved!")

    spark.stop()

  }

}
