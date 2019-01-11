package pe.com.belcorp.util

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.{Column, DataFrame}

object DataframeUtil {

  implicit class mdmDataFrame(df: DataFrame) {

    def fillDataframe(): DataFrame = {
      //delta and database data only have to get comunications data, not all data
      val columns = df.columns.distinct
      var df1 = df
      val columnsToHave = ConfigFactory.load().getConfig("fillColumns")
        .getStringList("columnsToHave").asScala.toArray
      columnsToHave.foreach { column =>
        if (!columns.contains(column)) df1 = df1.withColumn(column, lit("")) else df1
      }
      df1.na.fill("")
    }

    //SAP
    def sapFormat(): DataFrame = {
      // good enough
      val columnsToSelect = ConfigFactory.load().getConfig("sourcesSelect")
        .getStringList("sap").asScala.toArray
        .map(x => col(x))
      val df1 = df.select(columnsToSelect: _*)
        .withColumn("id", getIdS(col("codigo_material"), col("desc_material"), col("cod_grupo_articulo")))
        .drop("desc_material")
        .drop("cod_grupo_articulo")
      df1
    }

    def getIdS: UserDefinedFunction = udf((codigo_material: String, desc_material: String, cod_grupo_articulo: String) => {
      cod_grupo_articulo + " - " + codigo_material + " - " + desc_material
    })

    def sapApplyRules(): DataFrame = {
      val sapColumns = ConfigFactory.load()
        .getConfig("sourcesApplyRules")
        .getStringList("sap").asScala.toArray
      val columns = df.columns.distinct
      val selectMatchColumns = columns.map { column =>
        if (sapColumns.contains(column)) {
          column match {
            case "_id" => col(s"old.$column").alias(s"$column")
            case _ => col(s"new.$column").alias(s"$column")
          }
        }
        else {
          col(s"old.$column").alias(s"$column") //columns of webredes or comuniaciones
        }
      }
      df.select(selectMatchColumns: _*)
    }

    //COMUNICACIONES

    def comunicacionesFormat(): DataFrame = {
      val columnsToSelect = ConfigFactory.load().getConfig("sourcesSelect")
        .getStringList("comunicaciones").asScala.toArray
        .map(x => col(x))
      val df1 = df.select(columnsToSelect: _*).withColumn("titulo_descubre_mas_1", col("beneficio_slogan"))
        .withColumnRenamed("beneficio_slogan", "contenido_descripcion_1")
        .withColumn("codigo_categoria", lit("CMS"))
        .withColumn("talla_previa", getTallaPreviaC(col("desc_producto")))
      df1
    }

    def getTallaPreviaC: UserDefinedFunction = udf((descProducto: String) => {
      if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(descProducto, "Medidas")) {
        descProducto.substring(descProducto.indexOf("Medidas") + 9, descProducto.length)
      }
      else descProducto.toString
    })

    def comunicacionesApplyRules(): DataFrame = {
      //to modify, webRedesColumns coontiene columas ya formateadas despues del JOIN de porpias de comunicaciones y del match con webr

      val comunicacionesColumns = ConfigFactory.load()
        .getConfig("sourcesApplyRules")
        .getStringList("comunicaciones").asScala.toArray
      val columns = df.columns.distinct
      val selectMatchColumns = columns.map { column =>
        if (comunicacionesColumns.contains(column)) {
          column match {
            case "_id" => col(s"old.$column").alias(s"$column")
            case "nombre_producto" => productNameRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "desc_producto" => productDescriptionRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "titulo_descubre_mas_1" => titleRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "contenido_descripcion_1" => contentRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case _ => col(s"new.$column").alias(s"$column")
          }
        }
        else {
          col(s"old.$column").alias(s"$column") //columns of webredes
        }
      }
      df.select(selectMatchColumns: _*)
    }

    def productNameRuleC: (Column, Column) => Column = (colOld: Column, colNew: Column) => {
      when(col("old.codigo_categoria") === "CMS" || col("old.codigo_categoria") === "", colNew)
        .otherwise(colOld)
    }

    def productDescriptionRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.codigo_categoria") === "CMS" || col("old.codigo_categoria") === "", colNew)
        .otherwise(colOld)
    }

    def titleRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.codigo_categoria") === "CMS" || col("old.codigo_categoria") === "", colNew)
        .otherwise(colOld)
    }

    def contentRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.codigo_categoria") === "CMS" || col("old.codigo_categoria") === "", colNew)
        .otherwise(colOld)
    }

    //WEBREDES
    def webRedesFormat(): DataFrame = {
      val columnsToSelect = ConfigFactory.load().getConfig("sourcesSelect")
        .getStringList("comunicaciones").asScala.toArray
        .map(x => col(x))
      val df1 = df.select(columnsToSelect: _*).withColumn("link_video_1", getVideoLinkWR(col("link_video_1")))
        .withColumn("link_video_2", getVideoLinkWR(col("link_video_2")))
        .withColumn("link_video_3", getVideoLinkWR(col("link_video_3")))
        .withColumn("link_video_4", getVideoLinkWR(col("link_video_4")))
      df1
    }

    def getVideoLinkWR: UserDefinedFunction = udf(f = (videoLink: String) => {
      val youtubeRgx = """https?://(?:[0-9a-zA-Z-]+\.)?(?:youtu\.be/|youtube\.com\S*[^\w\-\s])([\w \-]{11})(?=[^\w\-]|$)(?![?=&+%\w]*(?:[\'"][^<>]*>|</a>))[?=&+%\w-]*""".r
      videoLink match {
        case youtubeRgx(a) => s"$a".toString
        case _ => videoLink.toString
      }
    })

    def webRedesApplyRules(): DataFrame = {
      //to modify, webRedesColumns coontiene columas ya formateadas despues del JOIN propias de wr y el match con comonuccaciones
      val webRedesColumns = ConfigFactory.load()
        .getConfig("sourcesApplyRules")
        .getStringList("webredes").asScala.toArray
      val columns = df.columns.distinct
      val selectMatchColumns = columns.map { column =>
        if (webRedesColumns.contains(column)) {
          column match {
            case "_id" => col(s"old.$column").alias(s"$column")
            case "nombre_producto" => productNameRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"column")
            case "desc_producto" => productDescriptionRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "titulo_descubre_mas_1" => titleRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "contenido_descripcion_1" => contentRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case _ => col(s"new.$column").alias(s"$column")
          }
        }
        else {
          col(s"old.${column}").alias(s"${column}") //columns of comunications
        }
      }
      df.select(selectMatchColumns: _*)
    }

    def productNameRuleWR = (colOld: Column, colNew: Column) => {
      when(col("new.codigo_categoria") !== "NA", colNew)
        .otherwise(colOld)
    }

    def productDescriptionRuleWR = (colOld: Column, colNew: Column) => {
      when(col("new.codigo_categoria") !== "NA", colNew)
        .otherwise(colOld)
    }

    def titleRuleWR = (colOld: Column, colNew: Column) => {
      when(col("new.codigo_categoria") !== "NA", colNew)
        .otherwise(colOld)
    }

    def contentRuleWR = (colOld: Column, colNew: Column) => {
      when(col("new.codigo_categoria") !== "NA", colNew)
        .otherwise(colOld)
    }
  }

}