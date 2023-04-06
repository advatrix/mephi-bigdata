
import lab1._
import eu.bitwalker.useragentutils.UserAgent
import org.apache.hadoop.io.{IntWritable, LongWritable, ObjectWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.MapDriver
import org.junit.Before
import org.junit.Test

import java.io.IOException
import org.junit.Assert.{assertEquals, assertTrue}

import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime, LocalTime}


class CountersTest {
  private var mapDriver: MapDriver[LongWritable, Text, Text, IntWritable] = _
  private val testMalformedIP = "mama mila ramu"
  private val testIP = "2023-04-05 14:55:47 [notice]\tapplication: bla bla bla"

  private val TestTime = new Text(
    LocalDate.parse("2023-04-05")
      atTime LocalTime.parse("14:55:47")
      truncatedTo ChronoUnit.HOURS
      format HW1Mapper.DateHourFormatter
  )

  @Before def setUp(): Unit = {
    val mapper = new HW1Mapper
    mapDriver = MapDriver.newMapDriver(mapper)
  }

  @Test
  @throws[IOException]
  def testMapperCounterOne(): Unit = {
    mapDriver.withInput(new LongWritable, new Text(testMalformedIP)).runTest()
    assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters.findCounter(Counter.Malformed, "").getValue)
  }

  @Test
  @throws[IOException]
  def testMapperCounterZero(): Unit = {
    mapDriver
      .withInput(new LongWritable, new Text(testIP))
      .withOutput(TestTime, new IntWritable(5))
      .runTest()

    assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters.findCounter(Counter.Malformed, "").getValue)
  }

  @Test
  @throws[IOException]
  def testMapperCounters(): Unit = {
    mapDriver
      .withInput(new LongWritable, new Text(testIP))
      .withInput(new LongWritable, new Text(testMalformedIP))
      .withInput(new LongWritable, new Text(testMalformedIP))
      .withOutput(TestTime, new IntWritable(5))
      .runTest()

    assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters.findCounter(Counter.Malformed, "").getValue)
  }
}