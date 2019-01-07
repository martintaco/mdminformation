package pe.com.belcorp

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import pe.com.belcorp.tables.MDMTables.CSVBase
import pe.com.belcorp.util.DataframeUtil._

object runMDM {

  val readConfig = ReadConfig(Map("uri" -> "mongodb+srv://adminCMQAS:xpqKt1EcRr3bIf3M@campaignmanager-k2qfs.mongodb.net/mdm_producto.ProductoTest?readPreference=primaryPreferred"))
  val writeConfig = WriteConfig(Map("uri" -> "mongodb+srv://adminCMQAS:xpqKt1EcRr3bIf3M@campaignmanager-k2qfs.mongodb.net/mdm_producto.ProductoTest"))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MDM").master("local[4]").getOrCreate()
    val fuente = "comunicaciones"
    fuente match {
      case "comunicaciones" => executeComunications(spark)
      case "webRedes" => executeWebRedes(spark)
    }
    spark.stop()
  }

  def executeComunications(spark: SparkSession): Unit = {
    val comunicacionesDF = new CSVBase("comunicaciones", "D:\\workspace\\MDM\\MDM\\data\\MDM_exported_Comunicaciones_clean.csv").get(spark)
      .comunicacionesFormat()
      .fillDataframe().as("new")
    val MDMtableDF = MongoSpark.load(spark, readConfig)
    if (MDMtableDF.head(1).isEmpty) {
      MongoSpark.save(comunicacionesDF.write.mode("append"), writeConfig)
    }
    else {
      val MDMtableOldDF = MDMtableDF.fillDataframe().as("old")
      val MDMjoinToInsertDF = comunicacionesDF.join(MDMtableOldDF, comunicacionesDF.col("codigo_material") === MDMtableOldDF.col("codigo_material"), "left_anti")
      MongoSpark.save(MDMjoinToInsertDF.write.mode("append"), writeConfig)
      val MDMjoinToupdateDF = comunicacionesDF.join(MDMtableOldDF, comunicacionesDF.col("codigo_material") === MDMtableOldDF.col("codigo_material"), "inner")
      val MDMjoinToupdateDF2 = MDMjoinToupdateDF.comunicacionesApplyRules()
      MongoSpark.save(MDMjoinToupdateDF2.write.mode("append"), writeConfig)
    }
  }

  def executeWebRedes(spark: SparkSession): Unit = {
    val webRedesDF = new CSVBase("webRedes", "D:\\workspace\\MDM\\MDM\\data\\MDM_exported_WebRedes_delta_clean.csv").get(spark)
      .webRedesFormat()
      .fillDataframe().as("new")
    val MDMtableDF = MongoSpark.load(spark, readConfig)
    if (MDMtableDF.head(1).isEmpty) {
      MongoSpark.save(webRedesDF.write.mode("append"), writeConfig)
    }
    else {
      val MDMtableOldDF = MDMtableDF.fillDataframe().as("old")
      val MDMjoinToInsertDF = webRedesDF.join(MDMtableOldDF, webRedesDF.col("codigo_material") === MDMtableOldDF.col("codigo_material"), "left_anti")
      MongoSpark.save(MDMjoinToInsertDF.write.mode("append"), writeConfig)
      val MDMjoinToupdateDF = webRedesDF.join(MDMtableOldDF, webRedesDF.col("codigo_material") === MDMtableOldDF.col("codigo_material"), "inner")
      val MDMjoinToupdateDF2 = MDMjoinToupdateDF.webRedesApplyRules()
      MongoSpark.save(MDMjoinToupdateDF2.write.mode("append"), writeConfig)
    }
  }

}
