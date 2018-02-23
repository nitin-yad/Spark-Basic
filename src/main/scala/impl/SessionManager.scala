package impl

import constants.Constants
import org.apache.spark.sql.SparkSession

/**
  * Created by nitin.yadav on 23-02-2018.
  */
object SessionManager {

    def getSparkSession(): SparkSession = {

      SparkSession.builder.appName(Constants.AppName).master(Constants.Master).getOrCreate()
    }
}
