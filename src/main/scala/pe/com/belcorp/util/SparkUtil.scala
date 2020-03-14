package pe.com.belcorp.util

import org.apache.spark.sql.SparkSession


object SparkUtil {

  def getSparkSession(appName: String): SparkSession = {
    //val spark = SparkSession.builder().appName(appName).master("local[4]").getOrCreate()
// PARA SERVIDOR
    val spark = SparkSession.builder().appName(appName).getOrCreate()

    //CONFIG TO REDSHIFT - S3 (LOCAL MODE)
    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "insert_access")
    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "insert_secret")
    spark
  }
}
