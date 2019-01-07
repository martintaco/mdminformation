package pe.com.belcorp.util

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.{Column, DataFrame}

object DataframeUtil {

  implicit class mdmDataFrame(df: DataFrame) {

    def fillDataframe(): DataFrame = {
      //delta and database data only have to get comunications data, not all data
      val columns = df.columns.distinct
      var df1 = df
      val columnsToHave = Array[String]("codigo_categoria", "fuente", "codigo_material", "nombre_producto", "capacidad", "talla_previa", "desc_producto", "titulo_tip_1", "descripcion_tip_1", "titulo_tip_2", "descripcion_tip_2", "titulo_tip_3", "descripcion_tip_3", "titulo_tip_4", "descripcion_tip_4", "titulo_descubre_mas_1", "contenido_descripcion_1", "titulo_descubre_mas_2", "contenido_descripcion_2", "titulo_descubre_mas_3", "contenido_descripcion_3", "titulo_paso_1", "descripcion_paso_1", "titulo_paso_2", "descripcion_paso_2", "titulo_paso_3", "descripcion_paso_3", "titulo_paso_4", "descripcion_paso_4", "link_video_1", "link_video_2", "link_video_3", "link_video_4", "nombre_articulo")
      columnsToHave.foreach { column =>
        if (!columns.contains(column)) df1 = df1.withColumn(column, lit("")) else df1
      }
      df1.na.fill("")
    }

    def comunicacionesFormat(): DataFrame = {
      // good enough
      val df1 = df.withColumn("titulo_descubre_mas_1", col("beneficio_slogan"))
        .withColumnRenamed("beneficio_slogan", "contenido_descripcion_1")
        .withColumn("codigo_categoria", lit("CMS"))
        .withColumn("talla_previa", getTallaPreviaC(col("desc_producto")))
      df1
    }

    //COMUNICACIONES

    def getTallaPreviaC: UserDefinedFunction = udf((descProducto: String) => {
      if (descProducto.contains("Medidas")) {
        descProducto.substring(descProducto.indexOf("Medidas") + 7, descProducto.length)
      }
      else descProducto.toString
    })

    def comunicacionesApplyRules(): DataFrame = {
      //to modify,
      val comunicacionesColumns = Array[String]("_id", "codigo_material", "nombre_producto", "nombre_articulo", "capacidad", "desc_producto", "titulo_descubre_mas_1", "contenido_descripcion_1")
      val columns = df.columns.distinct
      val selectMatchColumns = columns.map { column =>
        if (comunicacionesColumns.contains(column)) {
          column match {
            case "_id" => col(s"old.${column}").alias(s"${column}")
            case "nombre_producto" => productNameRuleC(col(s"old.${column}"), col(s"new.${column}")).alias(s"${column}")
            case "desc_producto" => productDescriptionRuleC(col(s"old.${column}"), col(s"new.${column}")).alias(s"${column}")
            case "titulo_descubre_mas_1" => titleRuleC(col(s"old.${column}"), col(s"new.${column}")).alias(s"${column}")
            case "contenido_descripcion_1" => contentRuleC(col(s"old.${column}"), col(s"new.${column}")).alias(s"${column}")
            case _ => col(s"new.${column}").alias(s"${column}")
          }
        }
        else {
          col(s"old.${column}").alias(s"${column}") //columns of webredes
        }
      }
      df.select(selectMatchColumns: _*)
    }

    def productNameRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.codigo_categoria") === "CMS", colNew)
        .otherwise(colOld)
    }

    def productDescriptionRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.codigo_categoria") === "CMS", colNew)
        .otherwise(colOld)
    }

    def titleRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.codigo_categoria") === "CMS", colNew)
        .otherwise(colOld)
    }

    def contentRuleC = (colOld: Column, colNew: Column) => {
      when(col("old.codigo_categoria") === "CMS", colNew)
        .otherwise(colOld)
    }

    def webRedesFormat(): DataFrame = {
      val df1 = df.withColumn("link_video_1", getVideoLinkWR(col("link_video_1")))
        .withColumn("link_video_2", getVideoLinkWR(col("link_video_2")))
        .withColumn("link_video_3", getVideoLinkWR(col("link_video_3")))
        .withColumn("link_video_4", getVideoLinkWR(col("link_video_4")))
      df1
    }

    //WEBREDES

    def getVideoLinkWR: UserDefinedFunction = udf(f = (videoLink: String) => {
      val youtubeRgx = """https?://(?:[0-9a-zA-Z-]+\.)?(?:youtu\.be/|youtube\.com\S*[^\w\-\s])([\w \-]{11})(?=[^\w\-]|$)(?![?=&+%\w]*(?:[\'"][^<>]*>|</a>))[?=&+%\w-]*""".r
      videoLink match {
        case youtubeRgx(a) => s"$a".toString
        case _ => videoLink.toString
      }
    })

    def webRedesApplyRules(): DataFrame = {
      //to modify,
      val comunicacionesColumns = Array[String]("_id", "codigo_categoria", "codigo_material", "nombre_producto", "nombre_articulo", "capacidad", "desc_producto", "titulo_descubre_mas_1", "contenido_descripcion_1")
      val columns = df.columns.distinct
      val selectMatchColumns = columns.map { column =>
        if (comunicacionesColumns.contains(column)) {
          column match {
            case "_id" => col(s"old.${column}").alias(s"${column}")
            case "nombre_producto" => productNameRuleWR(col(s"old.${column}"), col(s"new.${column}")).alias(s"${column}")
            case "desc_producto" => productDescriptionRuleWR(col(s"old.${column}"), col(s"new.${column}")).alias(s"${column}")
            case "titulo_descubre_mas_1" => titleRuleWR(col(s"old.${column}"), col(s"new.${column}")).alias(s"${column}")
            case "contenido_descripcion_1" => contentRuleWR(col(s"old.${column}"), col(s"new.${column}")).alias(s"${column}")
            case _ => col(s"new.${column}").alias(s"${column}")
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