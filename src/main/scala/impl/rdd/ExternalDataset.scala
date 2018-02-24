package impl.rdd

import constants.Constants
import impl.SessionManager

/**
  * Created by nitin.yadav on 23-02-2018.
  */
object ExternalDataset {

  /**
    * You can create rdd by reading data from any data source like HDFS, cassandra and even
    * from your local file system
    */

  def main(args: Array[String]) : Unit = {

    val sparkSession = SessionManager.getSparkSession()
    val fileRDD = sparkSession.sparkContext.textFile(Constants.TextFileLocation)

    /**
      * adding the size of all the lines
      */
    println(s"totalLength: ${fileRDD.map(line => line.length).reduce((a, b) => a + b)}")
  }
}
