package pe.com.belcorp

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import pe.com.belcorp.tables.SourceProcess
import pe.com.belcorp.util.Arguments
import pe.com.belcorp.util.SparkUtil._
import pe.com.belcorp.util.AWSCredentials

object runMDM {

  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val readConfig = ReadConfig(Map("uri" -> "mongodb+srv://adminBDInfoServiceQAS:FtzDmZ1hLx2yAFsU@bigdatainfoservice-0hd3l.mongodb.net/Analitico.Products?readPreference=primaryPreferred"))
  val writeConfig = WriteConfig(Map("uri" -> "mongodb+srv://adminBDInfoServiceQAS:FtzDmZ1hLx2yAFsU@bigdatainfoservice-0hd3l.mongodb.net/Analitico.Products"))

  def main(args: Array[String]): Unit = {
    val params = new Arguments(args)
    val spark = getSparkSession("MDM-info")
    val process = new SourceProcess(spark, params)
    params.source() match {
      case "sap" => process.executeSource("sap")
      case "comunicaciones" => process.executeSource("comunicaciones")
      case "webRedes" => process.executeSource("webRedes")
      case "all" => process.executeSource("sap")
        process.executeSource("comunicaciones")
        process.executeSource("webRedes")
      case _ => println("NOTHING TO DO")
    }
    spark.stop()
  }
 }
