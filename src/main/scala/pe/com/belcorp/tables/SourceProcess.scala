package pe.com.belcorp.tables

import org.apache.spark.sql.types.MetadataBuilder
import com.typesafe.config.ConfigFactory
import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import pe.com.belcorp.runMDM._
import pe.com.belcorp.tables.MDMTables.CSVBase
import pe.com.belcorp.util.Arguments
import pe.com.belcorp.util.DataframeUtil._

class SourceProcess(val spark: SparkSession, val params: Arguments) {

  def executeSource(source: String): Unit = {
    getNewDataframe(source) match {
      case Some(dataFrame) => processSource(dataFrame, source)
      case None =>
    }
  }

  def processSource(csvDF: DataFrame, source: String): Unit = {
    val csvDFnewChar = addingMaxColumnChar(csvDF,source)
    //COMMENT WRITE TO RS TO TEST PARA QUE FUNCIONES EL INSERT Y CREATE TABLE
    //writeToRedshift(csvDFnewChar, ConfigFactory.load().getString(s"redshiftConnection.${params.env()}"), ConfigFactory.load().getString(s"tempS3Directory.${params.env()}"), ConfigFactory.load().getString(s"redshiftTables.$source"))
    val sourceDF = newDataframeFormat(csvDF.na.fill(""), source) // uses empty  String instead of null values before the join
    sourceDF.show(20, false)
    val MDMtableDF = MongoSpark.load(spark, readConfig)
    if (emptyDT(MDMtableDF)) {
      MongoSpark.save(sourceDF.write.mode("append"), writeConfig)
    }
    else {
      val MDMtableOldDF = oldDataframeFormat(MDMtableDF).cache()
      insertNewRecordsToMongo(sourceDF, MDMtableOldDF)
      updateOldRecordsToMongo(sourceDF, MDMtableOldDF, source)
    }
  }

  def getNewDataframe(source: String): Option[DataFrame] = {
    var result: Option[DataFrame] = None
    try {
      val path = ConfigFactory.load().getString(s"${source}Path.${params.env()}") + "-" + params.date() + ".csv"
      val nuevo = new CSVBase(path).get(spark)
      nuevo.show(20, false)
      result = Some(nuevo)
    } catch {
      case e: Exception => {
        println(s"Error reading csv. reason: ${e}")
      }
    }
    result
  }

  def insertNewRecordsToMongo(sourceDF: DataFrame, MDMtableOldDF: DataFrame): Unit = {
    val MDMjoinToInsertDF = sourceDF.join(MDMtableOldDF, sourceDF.col("codsap") === MDMtableOldDF.col("codsap"), "left_anti")
    MongoSpark.save(MDMjoinToInsertDF.write.mode("append"), writeConfig)
  }

  def updateOldRecordsToMongo(sourceDF: DataFrame, MDMtableOldDF: DataFrame, source: String): Unit = {
    val MDMjoinToupdateDF = sourceDF.join(MDMtableOldDF, sourceDF.col("codsap") === MDMtableOldDF.col("codsap"), "inner")
    // MDMjoinToupdateDF.show(20, false)
    source match {
      case "comunicaciones" => MongoSpark.save(MDMjoinToupdateDF.comunicacionesApplyRules().write.mode("append"), writeConfig)
      case "webRedes" => MongoSpark.save(MDMjoinToupdateDF.webRedesApplyRules().write.mode("append"), writeConfig)
      case "sap" => MongoSpark.save(MDMjoinToupdateDF.sapApplyRules().write.mode("append"), writeConfig)
    }
  }

  def addingMaxColumnChar(dataFrame: DataFrame, source: String): DataFrame = {
    val metadata = new MetadataBuilder().putLong("maxlength", 1500).build()
    if (source == "webRedes") {
      val dataframeClean = dataFrame.withColumn("desdescubremas003", dataFrame("desdescubremas003").as("desdescubremas003", metadata))
        .withColumn("destip001", dataFrame("destip001").as("destip001", metadata))
        .withColumn("destip002", dataFrame("destip002").as("destip002", metadata))
        .withColumn("destip003", dataFrame("destip003").as("destip003", metadata))
        .withColumn("destip004", dataFrame("destip004").as("destip004", metadata))
        .withColumn("despaso001", dataFrame("despaso001").as("despaso001", metadata))
        .withColumn("despaso002", dataFrame("despaso002").as("despaso002", metadata))
        .withColumn("despaso003", dataFrame("despaso003").as("despaso003", metadata))
        .withColumn("despaso004", dataFrame("despaso004").as("despaso004", metadata))
        .withColumn("deswebredes", dataFrame("deswebredes").as("deswebredes", metadata))
        .withColumn("desdescubremas002", dataFrame("desdescubremas002").as("desdescubremas002", metadata))
        .withColumn("desdescubremas001", dataFrame("desdescubremas001").as("desdescubremas001", metadata))
        .withColumn("destitulotip001", dataFrame("destitulotip001").as("destitulotip001", metadata))
        .withColumn("destitulotip002", dataFrame("destitulotip002").as("destitulotip002", metadata))
        .withColumn("destitulotip003", dataFrame("destitulotip003").as("destitulotip003", metadata))
        .withColumn("destitulotip004", dataFrame("destitulotip004").as("destitulotip004", metadata))
      dataframeClean
    } else if (source == "comunicaciones") {
      val dataframeClean = dataFrame.withColumn("desmaterialmedidacatalogo", dataFrame("desmaterialmedidacatalogo").as("desmaterialmedidacatalogo", metadata))
      dataframeClean
    } else
      dataFrame
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
    //Para QAS y PRD antiguo
    //val targetTable = s"fnc_mdm.$writeTable"
    //PARA PRD Semantix
    val targetTable = s"fnc_analitico.$writeTable"
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
}