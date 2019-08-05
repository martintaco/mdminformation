package pe.com.belcorp

import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import pe.com.belcorp.tables.SourceProcess
import pe.com.belcorp.util.Arguments
import pe.com.belcorp.util.SparkUtil._
import pe.com.belcorp.util.AWSCredentials

object runMDM {

  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  //Para QAS 2 primeras líneas
  //val readConfig = ReadConfig(Map("uri" -> "mongodb+srv://adminBDInfoServiceQAS:FtzDmZ1hLx2yAFsU@bigdatainfoservice-0hd3l.mongodb.net/info_product.fichaproducto?readPreference=primaryPreferred"))
  //val writeConfig = WriteConfig(Map("uri" -> "mongodb+srv://adminBDInfoServiceQAS:FtzDmZ1hLx2yAFsU@bigdatainfoservice-0hd3l.mongodb.net/info_product.fichaproducto"))
  //Para PRD 2 lineas
  val readConfig = ReadConfig(Map("uri" -> "mongodb+srv://adminBDInfoServicePRD:9F1MWjK4Ktdzywk9@bigdatainfoservice-8x89f.mongodb.net/info_product.fichaproducto?readPreference=primaryPreferred"))
  val writeConfig = WriteConfig(Map("uri" -> "mongodb+srv://adminBDInfoServicePRD:9F1MWjK4Ktdzywk9@bigdatainfoservice-8x89f.mongodb.net/info_product.fichaproducto"))

  def main(args: Array[String]): Unit = {
    val params = new Arguments(args)
    val spark = getSparkSession("MDM-info2")
    val process = new SourceProcess(spark, params)
    params.source() match {
      case "comunicaciones" => process.executeSource("comunicaciones")
      case "sap" => process.executeSource("sap")
      case "webRedes" => process.executeSource("webRedes")
      case "all" => process.executeSource("comunicaciones")
        process.executeSource("webRedes")
        process.executeSource("sap")
      case _ => println("NOTHING TO DO")
    }
    spark.stop()
  }
 }
