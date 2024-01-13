import be.adamv.cz2.*

import java.io.{File, FileWriter}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Using



object DataParser extends Parser:
  override val empty: Int = 1
  override val singleton: Int = 2

  val symbols = RangeStorage[String](3, 100)
  val floats = RangeStorage[Float](2 << 24, 2 << 27)
  val strings = RangeStorage[String](2 << 27, 2 << 29)

  override def tokenizer(s: String): Option[Expr] =
    if s.head == '"' then Some(strings.addV(s))
    else if !s.head.isDigit then Some(symbols.addV(s))
    else Some(floats.addV(s.toFloat))


//enum GroundedData:
//  case NothingV
//  case IntV(value: Int)
//  case StringV(value: String)
//import GroundedData.*

//val evaluation: ValueEvaluationAlgorithms[GroundedData] = new ValueEvaluationAlgorithms[GroundedData]:
//  def handleLookup(emv: GroundedData, ev: GroundedData): GroundedData = if ev == NothingV then emv else ev
//
//  def handleMerge(fv: GroundedData, av: GroundedData): GroundedData = ???
//
//  def apply
//  (e: be.adamv.cz2.Expr)
//  (using s: be.adamv.cz2.ExprMap[GroundedData]):
//  be.adamv.cz2.ExprMap[GroundedData] = ???
//
//  def unapply
//  (e: be.adamv.cz2.Expr)
//  (using s: be.adamv.cz2.ExprMap[GroundedData]):
//  Option[(be.adamv.cz2.Expr, GroundedData)] = ???


//given PartialFunction[Int, ExprMap[GroundedData] => ExprMap[GroundedData]] = {
//  case 1000 => (em: ExprMap[GroundedData]) =>
//    ExprMap.from(em.items.map {
//      case (v, StringV(s)) =>
//        v -> IntV(s.length)
//      case p => p
//    })
//  case 1001 => (em: ExprMap[GroundedData]) =>
//    ExprMap(em.foldRight(Option(0)) {
//      case (IntV(k), Some(t)) => Some(t + k)
//      case _ => None
//    }.fold(invalidArgument -> NothingV)(t => Var(t) -> IntV(t)))
//}
//
//given em: ExprMap[GroundedData]()
//
//val ms = Loading.loadMessages("AdamHelderMeetTranscript.txt")
//
//def messageToExprs(m: Message, id: Int): List[(Expr, GroundedData)] =
//  val messageId = groundedOffset + m.message.hashCode.abs
//  val timeId = groundedOffset + (m.time.toSecondOfDay() - ms.head.time.toSecondOfDay())
//  val authorId = groundedOffset + m.author.hashCode.abs
//
//  List(
//    Expr(Var(100), Var(messageId), Var(messageId)) -> StringV(m.message),
//    Var(timeId) -> IntV(m.time.toSecondOfDay() - ms.head.time.toSecondOfDay()),
//    Var(authorId) -> StringV(m.author),
//    Expr(hasTime, Var(messageId), Var(timeId)) -> NothingV,
//    Expr(hasAuthor, Var(messageId), Var(authorId)) -> NothingV,
//  )
//
//val t0 = System.nanoTime()
//for (m, i) <- ms.zipWithIndex
//    case (e, v) <- messageToExprs(m, i) do
//  em.update(e, v)
//println(System.nanoTime() - t0)
//
//val query = Expr(hasAuthor, Var(0), Var(groundedOffset + ms(1).author.hashCode.abs)) -> Expr(stringLength, Var(-1))
//
//val t1 = System.nanoTime()
//val results = em.transform(query._1, query._2)
//println(System.nanoTime() - t1)
//
//results.items.take(10).foreach(println)
//
//val t2 = System.nanoTime()
//val processed = results.items.map((e, v) => evaluation.evalGrounded(e, v))
//println(System.nanoTime() - t2)
//
//processed.take(10).map(_.items.head).foreach(println)
//// lookup relies on `=` and does not fetch proper grounding
//println(evaluation.lookup(Var(2023876417), NothingV))
//
////  evaluation.evalGrounded(intSum, )


@main def example =
//  val lines = 67458171
  val lines = 1000000

  // TWO STEP
/*  val result = ArrayBuffer.fill[Expr](lines)(null)

  println("Parsing...")
  val t0 = System.nanoTime()

  Using(Source.fromFile(f"resources/edges$lines.metta"))(it =>
    val bit = it.buffered
    var last = DataParser.sexpr(bit)
    for i <- 0 until lines do
      val res = last.orNull
      if res == null then
        println(f"error occurred at $i")
      else
        result(i) = res
      if i % 10000 == 0 then println(i)
      last = DataParser.sexpr(bit)
  ).get

  println(f"Parsing took ${(System.nanoTime() - t0)/1e6} ms")
  println("Loading...")
  val t1 = System.nanoTime()

  val em = ExprMap[Unit]()

  for i <- 0 until lines do
    em.update(result(i), ())

  println(f"Loading took ${(System.nanoTime() - t1)/1e6} ms")*/

  // SINGLE STEP
  println("Loading and parsing...")
  val t1 = System.nanoTime()

  val em = ExprMap[Unit]()

  Using(Source.fromFile(f"resources/edges$lines.metta"))(it =>
    val bit = it.buffered
    var last = DataParser.sexpr(bit)
    for i <- 0 until lines do
      val res = last.orNull
      if res == null then
        println(f"error occurred at $i")
      else
        em.update(res, ())
      if i % 10000 == 0 then println(i)
      last = DataParser.sexpr(bit)
  ).get

  println(f"Parsing and loading took ${(System.nanoTime() - t1)/1e6} ms")


//  println("Dumping...")
//  val t2 = System.nanoTime()
//  val dump = em.prettyListing(false)
//  println(f"Dumping took ${(System.nanoTime() - t2) / 1e6} ms")
  println("Serializing...")
  val t3 = System.nanoTime()
  val serial = em.prettyStructuredSet(false)
  println(f"Serializing took ${(System.nanoTime() - t3) / 1e6} ms")

  val plain_file = new FileWriter(new File(f"resources/plain_edges$lines.metta"))
//  plain_file.write(dump)
  em.foreachKey(e => plain_file.write(PrettyPrinter.sexpression(e)))
  plain_file.close()

  val serial_file = new FileWriter(new File(f"resources/serial_edges$lines.metta"))
  serial_file.write(serial)
  serial_file.close()

  val symbol_values_file = new FileWriter(new File(f"resources/symbol_values$lines.metta"))
  val symbol_range = DataParser.symbols.start until (DataParser.symbols.start + DataParser.symbols.occupied)
  val symbols = symbol_range.map(DataParser.symbols.indexToValue).mkString("\n")
  symbol_values_file.write(symbols)
  symbol_values_file.close()
  val float_values_file = new FileWriter(new File(f"resources/float_values$lines.metta"))
  val float_range = DataParser.floats.start until (DataParser.floats.start + DataParser.floats.occupied)
  val floats = float_range.map(DataParser.floats.indexToValue).mkString("\n")
  float_values_file.write(floats)
  float_values_file.close()
  val string_values_file = new FileWriter(new File(f"resources/string_values$lines.metta"))
  val string_range = DataParser.strings.start until (DataParser.strings.start + DataParser.strings.occupied)
  val strings = string_range.map(DataParser.strings.indexToValue).mkString("\n")
  string_values_file.write(strings)
  string_values_file.close()
