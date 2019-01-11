package pe.com.belcorp.tables

import java.text.SimpleDateFormat
import java.util.Calendar
import com.typesafe.config.ConfigFactory
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.runMDM._
import pe.com.belcorp.tables.MDMTables.CSVBase
import pe.com.belcorp.util.Arguments
import pe.com.belcorp.util.DataframeUtil._

class SourceProcess(val spark: SparkSession, val params: Arguments) {

  def executeSource(source: String): Unit = {
    val sapAllDF = getNewDataframe(source)
    val sapDF = newDataframeFormat(sapAllDF, source)
    val MDMtableDF = MongoSpark.load(spark, readConfig)
    if (emptyDT(MDMtableDF)) {
      MongoSpark.save(sapDF.write.mode("append"), writeConfig)
    }
    else {
      val MDMtableOldDF = oldDataframeFormat(MDMtableDF).cache()
      insertNewRecordsToMongo(sapDF, MDMtableOldDF)
      updateOldRecordsToMongo(sapDF, MDMtableOldDF, source)
    }
  }

  def getNewDataframe(source: String): DataFrame = {
    val path = ConfigFactory.load().getString(s"csvPath.$source")
    new CSVBase(path).get(spark)
  }

  def insertNewRecordsToMongo(sourceDF: DataFrame, MDMtableOldDF: DataFrame): Unit = {
    val MDMjoinToInsertDF = sourceDF.join(MDMtableOldDF, sourceDF.col("codigo_material") === MDMtableOldDF.col("codigo_material"), "left_anti")
    //MongoSpark.save(MDMjoinToInsertDF.write.mode("append"), writeConfig)
  }

  def updateOldRecordsToMongo(sourceDF: DataFrame, MDMtableOldDF: DataFrame, source: String): Unit = {
    val MDMjoinToupdateDF = sourceDF.join(MDMtableOldDF, sourceDF.col("codigo_material") === MDMtableOldDF.col("codigo_material"), "inner")
    MDMjoinToupdateDF.show(20, false)
    source match {
      case "comunicaciones" => MongoSpark.save(MDMjoinToupdateDF.comunicacionesApplyRules().write.mode("append"), writeConfig)
      case "webRedes" => MongoSpark.save(MDMjoinToupdateDF.webRedesApplyRules().write.mode("append"), writeConfig)
      case "sap" => MongoSpark.save(MDMjoinToupdateDF.sapApplyRules().write.mode("append"), writeConfig)
    }
  }

  def newDataframeFormat(dataFrame: DataFrame, source: String): DataFrame = {
    source match {
      case "comunicaciones" => dataFrame.comunicacionesFormat().fillDataframe().as("new")
      case "webRedes" => dataFrame.webRedesFormat().fillDataframe().as("new")
      case "sap" => dataFrame.sapFormat().fillDataframe().as("new")
    }
  }

  def oldDataframeFormat(dataFrame: DataFrame): DataFrame = {
    dataFrame.fillDataframe().as("old")
  }

  def emptyDT(dataFrame: DataFrame): Boolean = {
    dataFrame.head(1).isEmpty
  }

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

  //https://stackoverflow.com/questions/31782763/how-to-use-regex-to-include-exclude-some-input-files-in-sc-textfile
  def getDate(): String = {
    val format = new SimpleDateFormat("dd-MM-yyyy")
    format.format(Calendar.getInstance().getTime())
  }
}
