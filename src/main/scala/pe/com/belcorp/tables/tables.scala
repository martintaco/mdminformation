package pe.com.belcorp.tables

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object MDMTables {

  class CSVBase( val path: String) {
    def get(spark: SparkSession): DataFrame = {
      spark.read
        .format("csv")
        .option("header", "true")
        .load(path)
/*        .select(columnsToSelect: _*)*/
        .cache()
    }

  }

}