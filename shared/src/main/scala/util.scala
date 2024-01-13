import scala.io.Source
import scala.util.Using


def summarizeResource[T](filename: String)(summarize: Iterator[String] => T): T =
  Using(Source.fromFile(s"resources/$filename"))(f =>
    summarize(f.getLines())
  ).get