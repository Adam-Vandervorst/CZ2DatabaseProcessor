import be.adamv.cz2.{Expr, *}
import cask.*

import java.io.{FileOutputStream, FileWriter}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.Files
import java.time.Instant
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.Duration


object DataParser extends Parser:
  override val empty: Int = 1
  override val singleton: Int = 2

  val symbols = ParRangeStorage[String](3, 2 << 24)
  val floats = ParRangeStorage[Float](2 << 24, 2 << 27)
  val strings = ParRangeStorage[String](2 << 27, 2 << 29)

  override def tokenizer(s: String): Expr =
    if s.head == '"' then strings.addV(s)
    else if !s.head.isDigit then symbols.addV(s)
    else floats.addV(s.toFloat)

object DataPrinter extends Printer:
  private val options: Array[String] = "abcdefghijklmnopqrstuvwxyz".toArray.map(x => x.toString)
  private val vcount = new ThreadLocal[Int]
  def newVarString: String =
    val c = vcount.get()
    vcount.set(c + 1)
    options(c)
  def preVarString(x: Long): String = options(-x.toInt)
  def freeVarString(x: Long): String =
    DataParser.symbols.get(x.toInt)
      .orElse(DataParser.symbols.get(x.toInt))
      .orElse(DataParser.floats.get(x.toInt).map(_.toString))
      .orElse(DataParser.strings.get(x.toInt).map('"' + _ + '"'))
//      .orElse(DataParser.strings.get(x.toInt))
      .getOrElse(x.toString)
  val exprSep: String = " "
  val exprOpen: String = "("
  val exprClose: String = ")"
  override def sexpression(e: Expr, depth: Int = 0, colored: Boolean = false): String =
    vcount.set(0)
    super.sexpression(e, depth, colored)

def parse_space(s: Iterable[Char]): ExprMap[Unit] =
  val result = ExprMap[Unit]()
  val it = s.iterator.buffered
  var last = DataParser.sexprUnsafe(it)
  while last ne null do
    result.update(last, ())
    last = DataParser.sexprUnsafe(it)
  result

def stringify_space(em: EM[_]): String =
  em.keys.map(DataPrinter.sexpression(_)).mkString("\n")

def parse_path(s: String): Seq[Option[Int]] =
  s.split('.').flatMap(x => Array(None, Some(DataParser.tokenizer(x).leftMost)))

def stringify_path(p: Seq[Option[Int]]): String =
  p.tail.map({case Some(i) => DataPrinter.freeVarString(i); case None => "."}).mkString

def wrap(p: Seq[Option[Int]], e: Expr): Expr =
  var inner = e
  for case (None, Some(i)) <- p.init.reverse zip p.reverse do
    inner = Expr.App(Expr.Var(i), inner)
  inner

type Path = Seq[Option[Int]]
val `*` = DataParser.symbols.addV("star")

enum Action:
  case Transform(source: Path, destination: Path, pattern: Expr, template: Expr)
  case BinOp(left: Path, right: Path, destination: Path, op: "union" | "intersection" | "subtraction")
  case ImportFile(destination: Path, scheme: "http" | "https" | "file", filetype: "metta" | "json" | "jsonl", filename: String, uri: URI)
  case ExportFile(source: Path, scheme: "http" | "https" | "file", filetype: "metta" | "json", filename: String)

  def serialized: Expr = this match
    case Action.Transform(source, destination, pattern, template) =>
      Expr(DataParser.symbols.addV("transform"),
           wrap(source, *),
           wrap(destination, *),
           Expr(pattern, template))
    case Action.BinOp(left, right, destination, op) =>
      Expr(DataParser.symbols.addV(op),
        wrap(left, *),
        wrap(right, *),
        wrap(destination, *))
    case ImportFile(destination, scheme, filetype, filename, uri) =>
      Expr(DataParser.symbols.addV("import"),
        wrap(destination, *),
        DataParser.strings.addV(uri.toString))
    case ExportFile(source, scheme, filetype, filename) =>
      Expr(DataParser.symbols.addV("export"),
        wrap(source, *),
        DataParser.strings.addV(scheme),
        DataParser.strings.addV(filetype),
        DataParser.strings.addV(filename))

  def execute(space: ExprMap[Unit]): Unit = this match
    case Action.Transform(source, destination, pattern, template) =>
      space.getAt(source) match
        case Left(em) =>
          space.setAt(destination, em.transform(pattern, template).em, ???)
        case Right(v) =>
          space.setAt(destination, ???, v)
    case Action.BinOp(left, right, destination, op) =>
      space.getAt(left) match
        case Left(em) =>
          space.getAt(right) match
            case Left(oem) =>
              space.setAt(destination, (op match
                case "union" => em.union(oem.asInstanceOf)
                case "intersection" => em.intersection(oem.asInstanceOf).em
                case "subtraction" => em.subtract(oem.asInstanceOf).em), ???)
            case Right(ov) => ()
        case Right(v) => ()
    case ImportFile(destination, scheme, filetype, filename, uri) =>
      scheme match
        case "http" | "https" =>
          filetype match
            case "metta" =>
              val response = requests.get.apply(uri.toURL.toString)
              val string = response.text()
              val future = Server.actorContext.future({
                val filepath = java.nio.file.Paths.get(s"resources/imports/metta/${stringify_path(destination).replace('.', '/')}/${filename}")
                filepath.getParent.toFile.mkdirs()
                Files.write(filepath, string.getBytes)
              })
              val subspace = parse_space(string)
              space.setAt(destination, subspace.em, ???)
              Await.result(future, Duration.Inf)
            case format => throw RuntimeException(f"format ${format} not supported yet (${this})")
        case "file" =>
          filetype match
            case "metta" =>
              val src_filepath = java.nio.file.Paths.get(uri.getPath)
              val future = Server.actorContext.future({
                val filepath = java.nio.file.Paths.get(s"resources/imports/metta/${stringify_path(destination).replace('.', '/')}/${filename}")
                filepath.getParent.toFile.mkdirs()
                Files.copy(src_filepath, filepath)
              })
              val string = Files.readString(src_filepath)
              val subspace = parse_space(string)
              space.setAt(destination, subspace.em, ???)
              Await.result(future, Duration.Inf)
            case format => throw RuntimeException(f"format ${format} not supported yet (${this})")
    case ExportFile(source, scheme, filetype, filename) =>
      scheme match
        case "http" =>
          filetype match
            case "metta" =>
              space.getAt(source) match
                case Left(em) =>
                  val s = em.size
                  val result = stringify_space(em)
                  println(s"serialized ${s} from path ${source}")
                  Files.write(java.nio.file.Paths.get(s"resources/exports/metta/${stringify_path(source).replace('.', '/')}/${filename}"), result.getBytes)
                case Right(v) =>
                  val result = v.toString
                  Files.write(java.nio.file.Paths.get(s"resources/exports/metta/${stringify_path(source).replace('.', '/')}/${filename}"), result.getBytes)
            case format => throw RuntimeException(f"format ${format} not supported yet (${this})")
        case scheme => throw RuntimeException(f"scheme ${scheme} not supported yet (${this})")

  def permissions: Seq[(Path, Boolean)] = this match
    case Action.Transform(source, destination, _, _) => Seq(source -> false, destination -> true)
    case Action.BinOp(left, right, destination, _) => Seq(left -> false, right -> false, destination -> true)
    case Action.ImportFile(destination, _, _, _, _) => Seq(destination -> true)
    case Action.ExportFile(source, _, _, _) => Seq(source -> false)


object Server extends cask.MainRoutes {
  private val space = ExprMap[Unit]()
  override def port: Int = 8081
  
  inline def instantExpr(): Expr =
    val instant = Instant.now()
    val discrim = (instant.getNano.toDouble/1_000_000_000.toDouble).toFloat
    Expr(DataParser.symbols.addV("Instant"), DataParser.strings.addV(Date.from(instant).toString), DataParser.floats.addV(discrim))

  private val registry_lock = java.util.concurrent.locks.ReentrantLock()
  private val registry = ArrayBuffer[(Long, Seq[Option[Int]], Boolean)]()
  def register(id: Long, actions: (Seq[Option[Int]], Boolean)*): Boolean =
    registry_lock.lock()
    try
      var possible = true
      for case (_, rp, rw) <- registry; case (ap, aw) <- actions do
        println(f"$rp, $ap, ${!ap.startsWith(rp)}")
        if rw || aw then possible &= !ap.startsWith(rp)
      println(s"possible ${possible} (registry: ${registry.mkString(";")}, requested: ${actions.mkString(";")})")
      if possible then registry.appendAll(actions.map((id, _, _)))
      possible
    finally registry_lock.unlock()
  def unregister(id: Long): Unit =
    registry_lock.lock()
    try
      registry.filterInPlace((aid, p, w) => if aid != id then true else { println(f"cleared $p $w"); false })
    finally registry_lock.unlock()
  inline def locked[A](inline actions: (Seq[Option[Int]], Boolean)*)(inline body: => A): String =
    val tid = Thread.currentThread().getId
    if register(tid, actions*) then
      try body.toString
      finally unregister(tid)
    else
      "conflicting reads at path"

  private val event_stream = new FileOutputStream("events.metta").getChannel
  event_stream.force(true)
  inline def writeBuf(inline serialization: => Array[Byte]): Int =
    val buf = serialization
    val lock = event_stream.lock()
    event_stream.write(ByteBuffer.wrap(buf))
    lock.release()
    buf.length

  inline def handleAction(inline request: cask.Request)(inline construction: => Action): String =
    try
      val action = construction
      actorContext.future(locked(action.permissions*){
        action.execute(space)
        val written = writeBuf {
          (DataPrinter.sexpression(
            Expr(instantExpr(),
              DataParser.symbols.addV("completed"),
              DataParser.strings.addV(request.exchange.getRequestId))
          ) + '\n').getBytes
        }
        println(f"completed ${request.exchange.getRequestId} ($written bytes written) ${action}")
      })
      val written = writeBuf {
        (DataPrinter.sexpression(
          Expr(instantExpr(),
               DataParser.symbols.addV("dispatched"),
               DataParser.strings.addV(request.exchange.getRequestId),
               action.serialized)
        ) + '\n').getBytes
      }
      f"dispatched ${request.exchange.getRequestId} ($written bytes written) ${action}"
    catch case e: Exception =>
      s"invalid request ${e.getMessage}"

  @cask.post("/modify/:destination")
  def modify(request: cask.Request, destination: String) =
    val write_path = parse_path(destination)
    val subspace = parse_space(request.text())
    println(s"request destination ${destination} (${write_path.mkString("::")}::Nil) ${write_path.map(_.map(DataPrinter.freeVarString(_)))}")
    val s = subspace.size
//    println("parsed")
//    println(subspace.prettyListing())
    locked(write_path -> true) {
      space.setAt(write_path, subspace.em, ???)
      println(s"and wrote ${s} to path ${destination}")
    }

  @cask.get("/view/:source")
  def view(request: cask.Request, source: String) =
    val read_path = parse_path(source)
    locked(read_path -> false) {
      space.getAt(read_path) match
        case Left(em) =>
          println(s"read from path ${source}")
          val s = em.size
          val result = stringify_space(em)
          println(s"and serialized ${s}")
          //        println(result)
          result
        case Right(v) => v.toString
    }

  @cask.get("/import/:destination")
  def importFile(request: cask.Request, destination: String, params: cask.QueryParams) =
    handleAction(request) {
      val uris = params.value("uri").head
      val uri = URI(uris)
      uri.getScheme match
        case "https" | "http" =>
          assert(Set("raw.githubusercontent.com", "localhost").contains(uri.getHost))
        case "file" => ()
      val filename = uris.splitAt(uris.lastIndexOf("/") + 1)._2
      val extension = filename.splitAt(filename.lastIndexOf(".") + 1)._2
      assert(Set("metta").contains(extension))
      Action.ImportFile(parse_path(destination), uri.getScheme.asInstanceOf, extension.asInstanceOf, filename, uri)
    }

  @cask.get("/export/:source/:scheme/:filename")
  def exportFile(request: cask.Request, source: String, scheme: String, filename: String) =
    handleAction(request) {
      val extension = filename.splitAt(filename.lastIndexOf(".") + 1)._2
      assert(Set("metta").contains(extension))
      assert(Set("file", "http").contains(scheme))
      Action.ExportFile(parse_path(source), extension.asInstanceOf, extension.asInstanceOf, filename)
    }

  @cask.staticFiles("/static/metta/")
  def staticFileMeTTa() = "resources/exports/metta"

  @cask.post("/transform/:source/:destination")
  def transform(request: cask.Request, source: String, destination: String) =
    handleAction(request) {
      DataParser.sexpr(request.text().iterator).get match
        case Expr.Var(v) => throw RuntimeException(s"expected an expression of the form (<pattern> <template>) and got $v")
        case Expr.App(pattern, template) => Action.Transform(parse_path(source), parse_path(destination), pattern, template)
    }

  @cask.get("/union/:left/:right/:destination")
  def union(request: cask.Request, left: String, right: String, destination: String) =
    handleAction(request) {
      Action.BinOp(parse_path(left), parse_path(right), parse_path(destination), "union")
    }

  @cask.get("/intersection/:left/:right/:destination")
  def intersection(request: cask.Request, left: String, right: String, destination: String) =
    handleAction(request) {
      Action.BinOp(parse_path(left), parse_path(right), parse_path(destination), "intersection")
    }

  @cask.get("/subtraction/:left/:right/:destination")
  def subtraction(request: cask.Request, left: String, right: String, destination: String) =
    handleAction(request) {
      Action.BinOp(parse_path(left), parse_path(right), parse_path(destination), "subtraction")
    }

  @cask.get("/stop/")
  def stopServer(request: Request) =
    val s = space.size
    println(f"contained $s atoms\n")
    actorContext.future(System.exit(0))
    f"contained $s atoms\n"

  initialize()
}

/*


curl localhost:8081/transform/edgar.imported/edgar.fuzzy-augmented/ -d "((FORM 3490 $company $formid $value) (signed $formid $date) (valuations $value $date $company))"
curl localhost:8081/union/edgar.imported/edgar.fuzzy-augmented/finance.db
curl -X POST localhost:8081/MORKL/queries.90431/
locs = DropHead(S"locations" <| S"Europe")
S"valuation" <| (interval(20_000_000, Inf) x interval(2006*year, 2007*year) x locs)
curl localhost:8081/view/queries.90431
25_000_000, April 2006, 23andMe Holding Co.
...

publishing cz2 (https://github.com/Adam-Vandervorst/CZ2):
sbt> project root
sbt> publishLocal

starting server (this repo):
sbt> project root
sbt> run
// todo evlog view
testing API (when located in https://github.com/trueagi-io/metta-examples/tree/main/aunt-kg):
curl localhost:8081/import/aunt-kg.toy/?uri=https://raw.githubusercontent.com/trueagi-io/metta-examples/refs/heads/main/aunt-kg/toy.metta
curl localhost:8081/view/aunt-kg.toy
curl localhost:8081/import/aunt-kg.simpsons/?uri=https://raw.githubusercontent.com/trueagi-io/metta-examples/refs/heads/main/aunt-kg/simpsons_simple.metta
curl localhost:8081/import/aunt-kg.lotr/?uri=https://raw.githubusercontent.com/trueagi-io/metta-examples/refs/heads/main/aunt-kg/lordOfTheRings_simple.metta
curl localhost:8081/union/aunt-kg.lotr/aunt-kg.simpsons/genealogy.merged
curl localhost:8081/union/aunt-kg.toy/genealogy.merged/genealogy.merged
curl localhost:8081/transform/genealogy.merged/genealogy.child-augmented/ -d "((parent \$x \$y) (child \$y \$x))"
curl localhost:8081/union/genealogy.merged/genealogy.child-augmented/genealogy.augmented
curl localhost:8081/export/genealogy.augmented > augmented.metta
curl localhost:8081/stop
*/
