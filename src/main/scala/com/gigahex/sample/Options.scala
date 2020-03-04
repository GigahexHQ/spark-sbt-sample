package com.gigahex.sample

case class Options(input: String = "./input.csv", output: String = "./output.csv")

trait BasicOptionParser {
  val prog = "spark-samples"

  private final val parser = new scopt.OptionParser[Options](prog) {
    head(prog,"1.0.0")

    opt[String]('i',"input").required().valueName("<input-path>")
      .action((x,c) => c.copy(input = x))
      .text("Input path")

    opt[String]('o',"output").valueName("<output-path>")
      .action((x,c) => c.copy(output = x))
      .text("Output path")

  }

  def parse(args : Array[String]) : Options = parser.parse(args,Options()) match {
    case Some(value) => value
    case None =>
      throw new IllegalArgumentException("Unable to parse the args")
  }
}

