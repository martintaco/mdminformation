package pe.com.belcorp.util

import java.text.SimpleDateFormat
import java.util.Calendar

import org.rogach.scallop._


class Arguments(arguments: Seq[String]) extends ScallopConf(arguments) {

  val hey = toggle(default=Some(false))
  val jdbc = opt[String]()
  val tempdir = opt[String]()
  val env = opt[String]()
  val source = opt[String](default = Some("all"))
  val date = opt[String](default = Some(getDate()))
  verify()


  def getDate(): String = {
    val format = new SimpleDateFormat("yyyyMMdd")
    format.format(Calendar.getInstance().getTime())
  }

}

