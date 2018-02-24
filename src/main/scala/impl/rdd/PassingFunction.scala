package impl.rdd

import constants.Constants
import impl.SessionManager

/**
  * Created by nitin.yadav on 24-02-2018.
  */
object PassingFunction {

  def main(args: Array[String]) : Unit = {

    val sparkSession = SessionManager.getSparkSession()
    val fileRDD = sparkSession.sparkContext.textFile(Constants.TextFileLocation)
    println(s"totalLength: ${fileRDD.map(MyFunction.func1).reduce(MyFunction.func2)}")
  }
}

object MyFunction {

  def func1(str : String) : Int = {

    str.length
  }

  def func2(a : Int, b : Int) : Int = {

    a + b
  }
}
