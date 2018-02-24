package impl.rdd

import constants.Constants
import impl.SessionManager

/**
  * Created by nitin.yadav on 24-02-2018.
  */
object Operations {

  /**
    * There are two types of operations:
    * 1. Transformation: created a new dataset from existing one e.g. map
    * 2. Action: returns a value e.g. reduce
    *
    * All transformations in spark are lazy. It just remembers the transformations applied
    * to a dataset. It is only computed when an action requires a result.
    */

  def main(args: Array[String]) : Unit = {

    val sparkSession = SessionManager.getSparkSession()
    val fileRDD = sparkSession.sparkContext.textFile(Constants.TextFileLocation)

    val lineLengthRDD = fileRDD.map(line => line.length)

    /**
      * RDD maybe computed each time you run an action on it. To avoid that you may use persist
      * or cache method.
      *
      */
    lineLengthRDD.persist()
    println(s"totalLength: ${lineLengthRDD.reduce((a, b) => a + b)}")
    println(s"maxLength: ${lineLengthRDD.reduce((a, b) => if(a > b) a else b)}")
  }
}
