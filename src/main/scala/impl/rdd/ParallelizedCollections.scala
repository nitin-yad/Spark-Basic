package impl.rdd

import impl.SessionManager

/**
  * Created by nitin.yadav on 23-02-2018.
  */
object ParallelizedCollections {

  /**
    * There are two ways to create a rdd. Create a collection and parallelize it
    * in your driver program is one of them.
    */

  def main(args : Array[String]) : Unit = {

    val sparkSession = SessionManager.getSparkSession()
    val nums = Array(1,1,2,3,4,5,6,7)
    val numsRDD = sparkSession.sparkContext.parallelize(nums)

    /**
      * You can create the number of partition you want manually by passing another
      * parameter to parallelize method, like this
      * val numsRDD = sparkSession.sparkContext.parallelize(nums, 9)
      */

    /**
      * We are adding all the number present in the array by this lambda expression
      * (a, b) => a + b)
      */
    println(s"sum: ${numsRDD.reduce((a, b) => a + b)}")
  }

}
