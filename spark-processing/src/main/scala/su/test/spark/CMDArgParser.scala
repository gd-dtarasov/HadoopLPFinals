package su.test.spark

object CMDArgParser {
  def parse(args: Array[String]): Args = {
    if (args.length < 2) {
      println("Not enough arguments. Expected <events-path> [<blocks-path> <locations-path>] <jdbc-url>")
      sys.exit(-1)
    }

    if (args.length == 4) {
      Args(args(0), Option(args(1)), Option(args(2)), args(3))
    } else {
      Args(args(0), Option.empty, Option.empty, args(args.length - 1))
    }
  }

  case class Args(eventsPath: String, blocksPath: Option[String], locationsPath: Option[String], jdbcURL: String)

}
