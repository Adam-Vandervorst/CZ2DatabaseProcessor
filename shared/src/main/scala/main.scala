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


@main def example =
//  val lines = 67458171
  val lines = 1000000

  // TWO STEP
  val result = ArrayBuffer.fill[Expr](lines)(null)

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

  println(f"Loading took ${(System.nanoTime() - t1)/1e6} ms")

  // SINGLE STEP
  /*println("Loading and parsing...")
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

  println(f"Parsing and loading took ${(System.nanoTime() - t1)/1e6} ms")*/


  println("Dumping...")
  val t2 = System.nanoTime()
  val dump = em.prettyListing(false)
  println(f"Dumping took ${(System.nanoTime() - t2) / 1e6} ms")
  println("Serializing...")
  val t3 = System.nanoTime()
  val serial = em.prettyStructuredSet(false)
  println(f"Serializing took ${(System.nanoTime() - t3) / 1e6} ms")

  val plain_file = new FileWriter(new File(f"resources/plain_edges$lines.metta"))
  plain_file.write(dump)
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
