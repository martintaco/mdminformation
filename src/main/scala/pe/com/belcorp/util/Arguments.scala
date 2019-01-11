package pe.com.belcorp.util

import org.rogach.scallop._


class Arguments(arguments: Seq[String]) extends ScallopConf(arguments) {

  val hey = toggle(default=Some(false))
  val jdbc = opt[String]()
  val tempdir = opt[String]()
  verify()

}