package pe.com.belcorp.tables

import org.apache.spark.sql.{DataFrame, SparkSession}

object MDMTables {

  class CSVBase(val path: String) {
    def get(spark: SparkSession): DataFrame = {
      spark.read
        .format("csv")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .load(path)//.na.fill("")  ya no va aqui por problemas con el insert en el driver
        /*        .select(columnsToSelect: _*)*/
        .cache()
    }
  }

}