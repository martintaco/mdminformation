package pe.com.belcorp

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.functions.col
import pe.com.belcorp.tables.MDMTables.CSVBase
import pe.com.belcorp.util.DataframeUtil._
import java.text.SimpleDateFormat
import java.util.Calendar
import pe.com.belcorp.util.Arguments
import pe.com.belcorp.util.SparkUtil._
import pe.com.belcorp.util.AWSCredentials
//import pe.com.belcorp.util.

object runMDM {

  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val readConfig = ReadConfig(Map("uri" -> "mongodb+srv://adminCMQAS:xpqKt1EcRr3bIf3M@campaignmanager-k2qfs.mongodb.net/mdm_producto.ProductoTest?readPreference=primaryPreferred"))
  val writeConfig = WriteConfig(Map("uri" -> "mongodb+srv://adminCMQAS:xpqKt1EcRr3bIf3M@campaignmanager-k2qfs.mongodb.net/mdm_producto.ProductoTest"))

  def main(args: Array[String]): Unit = {
    val params = new Arguments(args)
    val spark = getSparkSession("MDM-info")
    val fuente = "sap"
    fuente match {
      case "comunicaciones" => executeComunications(spark)
      case "webRedes" => executeWebRedes(spark)
      case "sap" => executeSap(spark, params)
    }
    spark.stop()
  }

  def executeSap(spark: SparkSession, params: Arguments): Unit = {
    val sapAllDF = new CSVBase("D:\\Users\\michaelmartin\\Desktop\\MDM\\data\\MDM-Atributos-SAP-20190105-0701545.csv").get(spark)
    sapAllDF.show(20, false)
    //writeToRedshift(sapAllDF, params.jdbc(), params.tempdir(), "mdm_producto_sap")

    val sapDF = sapAllDF.sapFormat().fillDataframe().as("new")
    val MDMtableDF = MongoSpark.load(spark, readConfig)
    if (MDMtableDF.head(1).isEmpty) {
      MongoSpark.save(sapDF.write.mode("append"), writeConfig)
    }
    else {
      val MDMtableOldDF = MDMtableDF.fillDataframe().as("old")
      val MDMjoinToInsertDF = sapDF.join(MDMtableOldDF, sapDF.col("codigo_material") === MDMtableOldDF.col("codigo_material"), "left_anti")
      MDMjoinToInsertDF.show(20, false)
      //MongoSpark.save(MDMjoinToInsertDF.write.mode("append"), writeConfig)
      val MDMjoinToupdateDF = sapDF.join(MDMtableOldDF, sapDF.col("codigo_material") === MDMtableOldDF.col("codigo_material"), "inner")
      MDMjoinToupdateDF.show(20, false)
      val MDMjoinToupdateDF2 = MDMjoinToupdateDF.sapApplyRules()
      MDMjoinToupdateDF2.show(20, false)
      MongoSpark.save(MDMjoinToupdateDF2.write.mode("append"), writeConfig)
    }
  }

  def executeComunications(spark: SparkSession): Unit = {
    val comunicacionesAllDF = new CSVBase("D:\\Users\\michaelmartin\\Desktop\\MDM\\data\\MDM_exported_Comunicaciones_delta_clean.csv").get(spark)

    /*Inserto to RS*/

    val comunicacionesDF = comunicacionesAllDF.comunicacionesFormat().fillDataframe().as("new")
    val MDMtableDF = MongoSpark.load(spark, readConfig)
    if (MDMtableDF.head(1).isEmpty) {
      MongoSpark.save(comunicacionesDF.write.mode("append"), writeConfig)
    }
    else {
      val MDMtableOldDF = MDMtableDF.fillDataframe().as("old")
      val MDMjoinToInsertDF = comunicacionesDF.join(MDMtableOldDF, comunicacionesDF.col("codigo_material") === MDMtableOldDF.col("codigo_material"), "left_anti")
      MDMjoinToInsertDF.show(20, false)
      MongoSpark.save(MDMjoinToInsertDF.write.mode("append"), writeConfig)
      val MDMjoinToupdateDF = comunicacionesDF.join(MDMtableOldDF, comunicacionesDF.col("codigo_material") === MDMtableOldDF.col("codigo_material"), "inner")
      MDMjoinToupdateDF.show(20, false)

      val MDMjoinToupdateDF2 = MDMjoinToupdateDF.comunicacionesApplyRules()
      MDMjoinToupdateDF2.show(20, false)
      MongoSpark.save(MDMjoinToupdateDF2.write.mode("append"), writeConfig)
    }
  }

  def executeWebRedes(spark: SparkSession): Unit = {

    val webRedesAllDF = new CSVBase("D:\\workspace\\MDM\\MDM\\data\\MDM_exported_WebRedes_delta_clean.csv").get(spark)

    /*Inserto to RS*/

    val webRedesDF = webRedesAllDF.webRedesFormat().fillDataframe().as("new")
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

  def readRedshift(spark: SparkSession, table: String): DataFrame = {
    //  val credentials = AWSCredentials.fetch() //not yet
    spark.read.format("com.databricks.spark.redshift")
      .option("jdbcdriver", "com.amazon.redshift.jdbc42.Driver")
      .option("url", "jdbc:redshift://10.12.0.94:5439/analitico?user=awsbelcorp&password=Belcorp2019") //params.jdbc()
      .option("tempdir", "s3a://belc-bigdata-domain-dlk-qas/dom-personalizacion/caida-promedio/tempRs/holi.txt") //params.tempdir()
      .option("dbtable", table) // params.table()
      .option("forward_spark_s3_credentials", true) // assume spark-s3 confs to redshift-s3 (LOCAL MODE)
      .option("tempformat", "CSV GZIP")
      .load()
  }

  //USING https://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html
  //BORING MODE, Letting all the work to RS
  def writeToRedshift(sourceDF: DataFrame, jdbcURL: String, tempS3Dir: String, writeTable: String): Unit = {
    val id = "codsap"
    val targetTable = s"fnc_mdm.$writeTable"
    val sourceTable = s"${targetTable}_source"
    val stagingTable = s"${targetTable}_staging"
    val getMatchRecords = s"INSERT INTO $stagingTable SELECT $sourceTable.* FROM $sourceTable JOIN $targetTable ON $targetTable.$id = $sourceTable.$id;"
    val updateMarchRecords = s"DELETE FROM $targetTable USING $stagingTable WHERE $targetTable.$id = $stagingTable.$id; INSERT INTO $targetTable SELECT * FROM $stagingTable;"
    val insertNewRecords = s"DELETE FROM $sourceTable USING $targetTable WHERE $sourceTable.$id = $targetTable.$id; INSERT INTO $targetTable SELECT * FROM $sourceTable;"
    val preActionQuery = s"CREATE TABLE $stagingTable (LIKE $targetTable)"
    val postActionsQuery = s"$getMatchRecords BEGIN TRANSACTION ; $updateMarchRecords $insertNewRecords END TRANSACTION ;DROP TABLE $sourceTable; DROP TABLE $stagingTable;"
    sourceDF.write.format("com.databricks.spark.redshift")
      .option("jdbcdriver", "com.amazon.redshift.jdbc42.Driver")
      .option("url", jdbcURL)
      .option("tempdir", tempS3Dir)
      .option("forward_spark_s3_credentials", true) // assume spark-s3 confs to redshift-s3 (LOCAL MODE)
      .option("preactions", s"$preActionQuery")
      .option("dbtable", sourceTable) //sourceTable
      .option("tempformat", "CSV GZIP")
      .option("postactions", s"$postActionsQuery")
      .mode("overwrite").save()
  }

  def getDate(): String = {
    val format = new SimpleDateFormat("dd-MM-yyyy")
    format.format(Calendar.getInstance().getTime())
  }

}
