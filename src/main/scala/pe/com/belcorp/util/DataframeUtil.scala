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
        .withColumn("id", getIdS(col("codsap"), col("desproductosap"), col("desgrupoarticulo")))
        .drop("desproductosap")
        .drop("desgrupoarticulo")
        /*.withColumnRenamed("CodSap", "codsap")*/
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
        .withColumnRenamed("destallamedidacatalogo","desproducto")
        .withColumnRenamed("desnombreelementocatalogo","desnombreproducto")
        .withColumn("destitulodescubremas001", col("desbeneficiosslogancatalogo"))
        .withColumnRenamed("desbeneficiosslogancatalogo", "desdescubremas001")
        .withColumn("codcategoria", lit("CMS"))
        .withColumn("tallaprevia", getTallaPreviaC(col("desproducto")))
      df1
    }

    def getTallaPreviaC: UserDefinedFunction = udf((descProducto: String) => {
      if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(descProducto, "Medidas")) {
        descProducto.substring(descProducto.indexOf("Medidas"), descProducto.length)
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
            case "desnombreproducto" => productNameRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "desproducto" => productDescriptionRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "destitulodescubremas001" => titleRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "desdescubremas001" => contentRuleC(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
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
      when(col("old.codcategoria") === "CMS" || col("old.codcategoria") === "", colNew)
        .otherwise(colOld)
    }

    def productDescriptionRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.codcategoria") === "CMS" || col("old.codcategoria") === "", colNew)
        .otherwise(colOld)
    }

    def titleRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.codcategoria") === "CMS" || col("old.codcategoria") === "", colNew)
        .otherwise(colOld)
    }

    def contentRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.codcategoria") === "CMS" || col("old.codcategoria") === "", colNew)
        .otherwise(colOld)
    }

    //WEBREDES
    def webRedesFormat(): DataFrame = {
      val columnsToSelect = ConfigFactory.load().getConfig("sourcesSelect")
        .getStringList("webRedes").asScala.toArray
        .map(x => col(x))
      val df1 = df.select(columnsToSelect: _*)
        .withColumnRenamed("codcategoriawebredes","codcategoria")
        .withColumnRenamed("desnombreproductowebredes","desnombreproducto")
        .withColumnRenamed("deswebredes","desproducto")
        .withColumn("deslinkvideoId001", getVideoLinkWR(col("deslinkvideo001")))
        .withColumn("deslinkvideoId002", getVideoLinkWR(col("deslinkvideo002")))
        .withColumn("deslinkvideoId003", getVideoLinkWR(col("deslinkvideo003")))
        .withColumn("deslinkvideoId004", getVideoLinkWR(col("deslinkvideo004")))
        .withColumn("deslinkvideoId005", getVideoLinkWR(col("deslinkvideo005")))
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
            case "desnombreproducto" => productNameRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "desproducto" => productDescriptionRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "destitulodescubremas001" => titleRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
            case "desdescubremas001" => contentRuleWR(col(s"old.$column"), col(s"new.$column")).alias(s"$column")
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
      when(col("new.codcategoria") !== "NA", colNew)
        .otherwise(colOld)
    }

    def productDescriptionRuleWR = (colOld: Column, colNew: Column) => {
      when(col("new.codcategoria") !== "NA", colNew)
        .otherwise(colOld)
    }

    def titleRuleWR = (colOld: Column, colNew: Column) => {
      when(col("new.codcategoria") !== "NA", colNew)
        .otherwise(colOld)
    }

    def contentRuleWR = (colOld: Column, colNew: Column) => {
      when(col("new.codcategoria") !== "NA", colNew)
        .otherwise(colOld)
    }
  }

}