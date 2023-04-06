package lab1

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.log4j.Logger

/**
 * Usage: ./program input1 [input2 ... inputN] output
 */
object MapReduceApplication extends App with Logging {
  if (args.length < 2)
    throw new RuntimeException("You should specify input and output folders!")
  val outputFolder :: inputFolders = args.map(new Path(_)).toSeq.reverse

  val conf = new Configuration

  val job = Job.getInstance(conf, "log severity aggregator")
  job.setJarByClass(this.getClass)
  job.setMapperClass(classOf[HW1Mapper])


  inputFolders foreach FileInputFormat.addInputPath(job, _)
  FileOutputFormat.setOutputPath(job, outputFolder)

  log.info("=====================JOB STARTED=====================")
  job.waitForCompletion(true)
  log.info("=====================JOB ENDED=====================")
  // проверяем статистику по счётчикам
  val counter = job.getCounters.findCounter("0", "0")
  log.info("=====================COUNTERS " + counter.getName + ": " + counter.getValue + "=====================")


  log.info("Hello world!")
}