import be.adamv.cz2
import be.adamv.cz2.*
import cask.*

import java.io.FileOutputStream
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer


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
      .orElse(DataParser.strings.get(x.toInt))
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

type Path = Seq[Option[Int]]

enum Action:
  case Transform(source: Path, destination: Path, pattern: Expr, template: Expr)
  case BinOp(left: Path, right: Path, destination: Path, op: "union" | "intersection" | "subtraction")

  def serialize(bb: ArrayBuffer[Byte]): Unit = this match
    case Action.Transform(source, destination, pattern, template) =>
      bb.appendAll("transform".getBytes)
      bb.append('/'.toByte)
      bb.appendAll(stringify_path(source).getBytes)
      bb.append('/'.toByte)
      bb.appendAll(stringify_path(destination).getBytes)
      bb.append('/'.toByte)
      bb.appendAll(DataPrinter.sexpression(Expr(pattern, template)).getBytes)
    case Action.BinOp(left, right, destination, op) =>
      bb.appendAll(op.getBytes)
      bb.append('/'.toByte)
      bb.appendAll(stringify_path(left).getBytes)
      bb.append('/'.toByte)
      bb.appendAll(stringify_path(right).getBytes)
      bb.append('/'.toByte)
      bb.appendAll(stringify_path(destination).getBytes)
      bb.append('/'.toByte)

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

  def permissions: Seq[(Path, Boolean)] = this match
    case Action.Transform(source, destination, pattern, template) => Seq(source -> false, destination -> true)
    case Action.BinOp(left, right, destination, op) => Seq(left -> false, right -> false, destination -> true)


object Server extends cask.MainRoutes {
  private val space = ExprMap[Unit]()
  override def port: Int = 8081

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

  private val event_stream = new FileOutputStream("event.log").getChannel
  event_stream.force(true)
  inline def writeBuf(inline serialization: => ArrayBuffer[Byte]): Int =
    val buf = serialization
    val lock = event_stream.lock()
    event_stream.write(ByteBuffer.wrap(buf.toArray))
    lock.release()
    buf.length

  inline def handleAction(inline request: cask.Request)(inline construction: => Action): String =
    try
      val action = construction
      actorContext.future(locked(action.permissions*){
        action.execute(space)
        val bb = ArrayBuffer.empty[Byte]
        val written = writeBuf {
          bb.appendAll(request.exchange.getRequestId.getBytes)
          bb.append('\n'.toByte)
          bb
        }
        println(f"completed ${request.exchange.getRequestId} ($written bytes written) ${action}")
      })
      val bb = ArrayBuffer.empty[Byte]
      val written = writeBuf {
        bb.appendAll(System.currentTimeMillis().toString.getBytes)
        bb.append(','.toByte)
        bb.appendAll(request.exchange.getRequestId.getBytes)
        bb.append(','.toByte)
        action.serialize(bb)
        bb.append('\n'.toByte)
        bb
      }
      f"dispatched ${request.exchange.getRequestId} ($written bytes written) ${action}"
    catch case e: Exception =>
      s"invalid request ${e.getMessage}"

  @cask.post("/import/:destination")
  def importFile(request: cask.Request, destination: String) =
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

  @cask.get("/export/:source")
  def exportFile(request: cask.Request, source: String) =
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
curl localhost:8081/union/aunt-kg.lotr/aunt-kg.simpsons/genealogy.merged
curl localhost:8081/union/aunt-kg.toy/genealogy.merged/genealogy.merged
curl localhost:8081/transform/genealogy.merged/genealogy.child-augmented/ -d "((parent \$x \$y) (child \$y \$x))"
curl localhost:8081/union/genealogy.merged/genealogy.child-augmented/genealogy.augmented
curl localhost:8081/export/genealogy.augmented > augmented.metta
curl localhost:8081/stop
*/
