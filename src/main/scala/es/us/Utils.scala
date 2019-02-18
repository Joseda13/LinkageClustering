package es.us

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD

/**
  * This object contains two methods that calculates the optimal number for
  * clustering using Kmeans in SPARK MLLIB
  *
  * @author José María Luna
  * @version 1.0
  * @since v1.0 Dev
  */
object Utils {

  /**
    * Calculate the Euclidena distance between tow points
    * @param v1    First Vector in Sequence to Double
    * @param v2 Second Vector in Sequence to Double
    * @return Return the Euclidena distance between tow points
    * @example distEuclidean(v1, v2)
    */
  def distEuclidean(v1: Seq[Double], v2: Seq[Double]): Double = {
    require(v1.size == v2.size, s"Vector dimensions do not match: Dim(v1)=${v1.size} and Dim(v2)" +
      s"=${v2.size}.")
    var squaredDistance = 0.0

    var kv = 0
    val sz = v1.size
    while (kv < sz) {
      val score = v1.apply(kv) - v2.apply(kv)
      squaredDistance += Math.abs(score*score)
      kv += 1
    }
    math.sqrt(squaredDistance)
  }

  def whatTimeIsIt(): String = {
     new SimpleDateFormat("yyyyMMddhhmm").format(Calendar.getInstance().getTime())
  }

  def whatDayIsIt(): String = {
     new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime())
  }

  //Return 0 if the data is empty, else return data parsed to Double
  def dataToDouble(s: String): Double = {
    if (s.isEmpty) 0 else s.toDouble
  }

  def calculateMedian(listado: List[Double]): Double = {

    val count = listado.length

    val median: Double = if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (listado.apply(l) + listado.apply(r)) / 2
    } else listado.apply(count / 2)

     median

  }

  def printRDD(dataRDD: RDD[Unit], nameFile: String): Unit = {
    new PrintWriter(nameFile) {
      dataRDD.foreach(println)
      close()
    }
  }

}
