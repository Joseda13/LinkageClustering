package es.us

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import es.us.linkage.{Distance, Linkage}
import org.apache.spark.sql.functions.monotonically_increasing_id

object MainLinkageExecution {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName(s"Linkage")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //Configuration settings
    var pathToDatabase = "B:\\Datasets\\Test_Varios\\C3-D20-I100"
    var delimiter = ","
    var numCluster = 1
    var numPoints = 300
    var linkageStrategy = "avg"
    var pathToCheckPoint = "B:\\checkpoints"
    var pathToLinkageModel = ""

    if (args.length > 2){
      pathToDatabase = args.apply(0)
      delimiter = args.apply(1)
      numCluster = args.apply(2).toInt
      numPoints = args.apply(3).toInt
      linkageStrategy = args.apply(4)
      pathToCheckPoint = args.apply(5)
      pathToLinkageModel = args.apply(6)
    }

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

    //Execution linkage clustering algorithm
    val linkage = new Linkage(numCluster, linkageStrategy)

    println("*** Linkage Clustering ***")
    println("*** K = " + numCluster + " ***")
    println("*** Points = " + numPoints + "***")
    println("*** Strategy = " + linkageStrategy + "***")
    println("Executing Linkage Clustering Algorithm ...")
    val clustering = linkage.runAlgorithm(distancesRDD, numPoints)
//    val clustering = linkage.runAlgorithmDendrogram(distancesRDD, numPoints, numCluster)

    println("Saving the model ...")
    //Save the result
    clustering._1.saveSchema(pathToLinkageModel)
    println("Model saved!")

    spark.stop()
  }
}
