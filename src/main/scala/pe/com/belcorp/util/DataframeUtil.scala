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
        .withColumn("id", getIdS(col("CodSap"), col("DesProductoSap"), col("DesGrupoArticulo")))
        .drop("DesProductoSap")
        .drop("DesGrupoArticulo")
        .withColumnRenamed("CodSap", "cod_material")
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
      val df1 = df.select(columnsToSelect: _*)
        .withColumnRenamed("des_talla_medida_catalogo","des_producto")
        .withColumnRenamed("des_nombre_elemento_catalogo","des_nombre_producto")
        .withColumn("des_titulo_descubremas_001", col("des_beneficios_slogan_catalogo"))
        .withColumnRenamed("des_beneficios_slogan_catalogo", "des_descubremas_001")
        .withColumn("cod_categoria", lit("CMS"))
        .withColumn("talla_previa", getTallaPreviaC(col("des_producto")))
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
            case "des_nombre_producto" => productNameRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "des_producto" => productDescriptionRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "des_titulo_descubremas_001" => titleRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "des_descubremas_001" => contentRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
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
      when(col("old.cod_categoria") === "CMS" || col("old.cod_categoria") === "", colNew)
        .otherwise(colOld)
    }

    def productDescriptionRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.cod_categoria") === "CMS" || col("old.cod_categoria") === "", colNew)
        .otherwise(colOld)
    }

    def titleRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.cod_categoria") === "CMS" || col("old.cod_categoria") === "", colNew)
        .otherwise(colOld)
    }

    def contentRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.cod_categoria") === "CMS" || col("old.cod_categoria") === "", colNew)
        .otherwise(colOld)
    }

    //WEBREDES
    def webRedesFormat(): DataFrame = {
      val columnsToSelect = ConfigFactory.load().getConfig("sourcesSelect")
        .getStringList("webRedes").asScala.toArray
        .map(x => col(x))
      val df1 = df.select(columnsToSelect: _*)
        .withColumnRenamed("cod_categoria_webredes","cod_categoria")
        .withColumnRenamed("des_nombre_producto_webredes","des_nombre_producto")
        .withColumnRenamed("des_webredes","des_producto")
        .withColumn("des_link_video_001", getVideoLinkWR(col("des_link_video_001")))
        .withColumn("des_link_video_002", getVideoLinkWR(col("des_link_video_002")))
        .withColumn("des_link_video_003", getVideoLinkWR(col("des_link_video_003")))
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
        .getStringList("webRedes").asScala.toArray
      val columns = df.columns.distinct
      val selectMatchColumns = columns.map { column =>
        if (webRedesColumns.contains(column)) {
          column match {
            case "_id" => col(s"old.$column").alias(s"$column")
            case "des_nombre_producto" => productNameRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "des_producto" => productDescriptionRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "des_titulo_descubremas_001" => titleRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "des_descubremas_001" => contentRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case _ => col(s"new.$column").alias(s"$column")
          }
        }
        else {
          col(s"old.$column").alias(s"$column") //columns of comunications
        }
      }
      df.select(selectMatchColumns: _*)
    }

    def productNameRuleWR = (colOld: Column, colNew: Column) => {
      when(col("new.cod_categoria") !== "NA", colNew)
        .otherwise(colOld)
    }

    def productDescriptionRuleWR = (colOld: Column, colNew: Column) => {
      when(col("new.cod_categoria") !== "NA", colNew)
        .otherwise(colOld)
    }

    def titleRuleWR = (colOld: Column, colNew: Column) => {
      when(col("new.cod_categoria") !== "NA", colNew)
        .otherwise(colOld)
    }

    def contentRuleWR = (colOld: Column, colNew: Column) => {
      when(col("new.cod_categoria") !== "NA", colNew)
        .otherwise(colOld)
    }
  }

}