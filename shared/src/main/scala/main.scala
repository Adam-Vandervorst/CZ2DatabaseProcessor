import be.adamv.cz2.*
import cask.*


object DataParser extends Parser:
  override val empty: Int = 1
  override val singleton: Int = 2

  val symbols = RangeStorage[String](3, 100)
  val floats = RangeStorage[Float](2 << 24, 2 << 27)
  val strings = RangeStorage[String](2 << 27, 2 << 29)

  override def tokenizer(s: String): Expr =
    if s.head == '"' then strings.addV(s)
    else if !s.head.isDigit then symbols.addV(s)
    else floats.addV(s.toFloat)

object DataPrinter extends Printer:
  val newVarString: String = "◆"
  def preVarString(x: Long): String = "⏴" + subscript(-x.toInt)
  def freeVarString(x: Long): String =
    DataParser.symbols.get(x.toInt)
      .orElse(DataParser.symbols.get(x.toInt))
      .orElse(DataParser.floats.get(x.toInt).map(_.toString))
      .orElse(DataParser.strings.get(x.toInt))
      .getOrElse(x.toString)
  val exprSep: String = " "
  val exprOpen: String = "("
  val exprClose: String = ")"

def parse_space(s: Iterable[Char]): ExprMap[Unit] =
  val result = ExprMap[Unit]()
  val it = s.iterator.buffered
  var last = DataParser.sexprUnsafe(it)
  while last ne null do
    result.update(last, ())
    last = DataParser.sexprUnsafe(it)
  result

def stringify_space(em: EM[_]): String =
  em.keys.map(DataPrinter.sexpression(_, 0, false)).mkString("\n")

def parse_path(s: String): Iterable[Option[Int]] =
  s.split('.').flatMap(x => Array(None, Some(DataParser.tokenizer(x).leftMost)))

object Server extends cask.MainRoutes {
  var space = ExprMap[Unit]()
  override def port: Int = 8081

  @cask.post("/import/:destination")
  def importFile(request: cask.Request, destination: String) = {
    val write_path = parse_path(destination)
    val subspace = parse_space(request.text())
//    println(s"request destination ${destination} (${write_path.mkString("::")}::Nil)")
//    println("parsed")
//    println(subspace.prettyListing())
    space.setAt(write_path, subspace.em, ???)
//    println(s"and wrote it to path ${destination}")
  }

  @cask.get("/export/:source")
  def exportFile(request: cask.Request, source: String) = {
    val read_path = parse_path(source)
    space.getAt(read_path) match
      case Left(em) =>
//        println(s"read from path ${source}")
        val result = stringify_space(em)
//        println("and serialized it")
//        println(result)
        result
      case Right(v) => v.toString
  }

  @cask.post("/transform/:source/:destination")
  def transform(request: cask.Request, source: String, destination: String) = {
    val read_path = parse_path(source)
    val write_path = parse_path(destination)
    val App(pattern, template) = DataParser.sexpr(request.text().iterator).get: @unchecked

    space.getAt(read_path) match
      case Left(em) =>
        val transformed_subspace = em.transform(pattern, template)
        space.setAt(write_path, transformed_subspace.em, ???)
      case Right(v) =>
        space.setAt(write_path, ???, v)
  }

  @cask.get("/union_into/:left/:right/:destination")
  def unionInto(request: cask.Request, left: String, right: String, destination: String) = {
    val read_left_path = parse_path(left)
    val read_right_path = parse_path(right)
    val write_path = parse_path(destination)
    space.getAt(read_left_path) match
      case Left(em) => space.getAt(read_right_path) match
        case Left(oem) =>
          space.setAt(write_path, em.union(oem.asInstanceOf), ???)
        case Right(ov) => ()
      case Right(v) => ()
  }

  @cask.get("/intersection_into/:left/:right/:destination")
  def intersectionInto(request: cask.Request, left: String, right: String, destination: String) = {
    val read_left_path = parse_path(left)
    val read_right_path = parse_path(right)
    val write_path = parse_path(destination)
    space.getAt(read_left_path) match
      case Left(em) => space.getAt(read_right_path) match
        case Left(oem) =>
          space.setAt(write_path, em.intersection(oem.asInstanceOf).em, ???)
        case Right(ov) => ()
      case Right(v) => ()
  }

  @cask.get("/subtraction_from/:left/:right/:destination")
  def subtractionFrom(request: cask.Request, left: String, right: String, destination: String) = {
    val read_left_path = parse_path(left)
    val read_right_path = parse_path(right)
    val write_path = parse_path(destination)
    space.getAt(read_left_path) match
      case Left(em) => space.getAt(read_right_path) match
        case Left(oem) =>
          space.setAt(write_path, oem.subtract(em.asInstanceOf).em, ???)
        case Right(ov) => ()
      case Right(v) => ()
  }

  @cask.get("/stop/")
  def stopServer(request: Request) = {
    val s = space.size
    actorContext.future(System.exit(0))
    f"contained $s atoms\n"
  }

  initialize()
}

/*
publishing cz2 (https://github.com/Adam-Vandervorst/CZ2):
sbt> project root
sbt> publishLocal

starting server (this repo):
sbt> project root
sbt> run

testing API (when located in https://github.com/trueagi-io/metta-examples/tree/main/aunt-kg):
curl -i -X POST localhost:8081/import/aunt-kg.toy -d "@toy.metta"
curl localhost:8081/export/aunt-kg.toy
curl -i -X POST localhost:8081/import/aunt-kg.simpsons -d "@simpsons_simple.metta"
curl -i -X POST localhost:8081/import/aunt-kg.lotr -d "@lordOfTheRings_simple.metta"
curl localhost:8081/union_into/aunt-kg.lotr/aunt-kg.simpsons/genealogy.merged
curl localhost:8081/union_into/aunt-kg.toy/genealogy.merged/genealogy.merged
curl localhost:8081/export/genealogy.merged
curl localhost:8081/stop
*/
