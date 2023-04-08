package lab1

import lab1.HW1Mapper.DateHourFormatter
import org.apache.hadoop.io.{IntWritable, LongWritable, MapWritable, ObjectWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import java.io.IOException
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.util.Try

class HW1Mapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  private val UnknownSeverityLevel = -1
  @throws[IOException]
  @throws[InterruptedException]
  override protected def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val (time, severityLevel) =
      value.toString.split("\\s").take(3).toList match {
        case rawDate :: rawTime :: rawSeverity :: _ =>
          val time = Try(
            Some(LocalDate.parse(rawDate) atTime LocalTime.parse(rawTime) truncatedTo ChronoUnit.HOURS)
          ).toOption.flatten

          val severityLevel = rawSeverity dropRight 1 drop 1 match {
            case "emerg" | "panic" => 0
            case "alert" => 1
            case "crit" => 2
            case "err" | "error" => 3
            case "warn" | "warning" => 4
            case "notice" => 5
            case "info" => 6
            case "debug" => 7
            case _ => UnknownSeverityLevel
          }

          (time, severityLevel)
        case _ =>
          (None, UnknownSeverityLevel)
      }

    if (severityLevel == UnknownSeverityLevel)
      context.getCounter(Counter.Malformed, "").increment(1)
    else
      context.write(new Text(time.map(_.format(DateHourFormatter)) getOrElse ""), new IntWritable(severityLevel))
  }
}

object HW1Mapper {
  lazy val DateHourFormatter: DateTimeFormatter = DateTimeFormatter ofPattern "dd.MM.yyyy HH"
}

case class TimeWritable(time: LocalDateTime) extends ObjectWritable

object Counter {
  final val Malformed: String = "Malformed"
}
