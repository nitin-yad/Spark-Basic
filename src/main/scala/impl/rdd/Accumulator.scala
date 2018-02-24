package impl.rdd

import impl.SessionManager

/**
  * Created by nitin.yadav on 24-02-2018.
  */

/**
  * Accumulator are like shared variables. Tasks running on a cluster can add to it using the add method.
  * However, they cannot read its value. Only the driver program can read accumulator's value.
  */
class Accumulator {

  def main(args: Array[String]): Unit = {

    val sparkSession = SessionManager.getSparkSession()
    val accumulator = sparkSession.sparkContext.longAccumulator("Long Accumulator")
    sparkSession.sparkContext.parallelize(Array(1, 2, 3, 4, 5)).foreach(x => accumulator.add(x))
    println(s"Accumulator value: ${accumulator.value}")
  }
}