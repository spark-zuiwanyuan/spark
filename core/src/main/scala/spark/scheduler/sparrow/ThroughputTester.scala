/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.scheduler.sparrow

import java.io.{File, PrintWriter}
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.math

import spark._
import SparkContext._

/**
 * Launches jobs composed of sleep tasks at a fixed rate.
 *
 * Intended for use to test scheduler throughput.
 */
object ThroughputTester {
  var responseTimes = new ArrayBuffer[Int]() with SynchronizedBuffer[Int]

  def get_percentile(values: ArrayBuffer[Int], percent: Double): Double = {
    val index = (values.size - 1) * percent
    val below = math.floor(index).toInt
    val above = math.ceil(index).toInt
    if (below == above) {
      return values(below)
    }
    val d0 = values(below) * (above - index)
    val d1 = values(above) * (index - below)
    return d0 + d1
  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println(
        "Usage: ThroughputTester master total_cores")
      System.exit(1)
    }
    val tasksPerJob = 10
    val taskDurations = List(10000, 1000, 100, 10)
    val totalCores = args(1).toInt
    val load = 0.8

    val sc = new SparkContext(args(0), "ThroughputTester", System.getenv("SPARK_HOME"), Seq())
    println("Sleeping to give everything time to start up")
    Thread.sleep(10000)
    println("Done sleeping; launching warmup job")
    sc.parallelize(1 to 100000, 100000).map(x => Thread.sleep(1)).count()
    println("Warmup job complete")

    taskDurations.foreach { taskDurationMillis =>
      println("****************Launching experiment with %s millisecond tasks".format(
        taskDurationMillis))
      val interarrivalDelay = tasksPerJob * taskDurationMillis / (totalCores * load)
      println(
        ("Launching tasks with interarrival delay %sms (to sustain load %s on %s cores)").
         format(interarrivalDelay, load, totalCores))
      val pool = new ScheduledThreadPoolExecutor(200)
      // Can't uuse scheudleAtFixedRate because each runnable may take longer than
      // the interarrival delay, which is not supported.
      // Start after a delay, so that we can schedule everything before any queries start.
      var startDelay = 20 * 1000
      var cumulativeDelay = 0.0
      val experimentDurationMillis = 5 * 60 * 1000
      while (cumulativeDelay < experimentDurationMillis) {
        val queryRunnable = new QueryLaunchRunnable(sc, tasksPerJob, taskDurationMillis)
        val delay = ((startDelay + cumulativeDelay) * 1000000).toLong
        pool.schedule(queryRunnable, delay.toLong, TimeUnit.NANOSECONDS)
        cumulativeDelay += interarrivalDelay
      }
      Thread.sleep(experimentDurationMillis + startDelay)
      pool.shutdown()

      if (responseTimes.size == 0) return
      val sortedResponseTimes = responseTimes.sortWith(_ < _)
      val writer = new PrintWriter(new File("throughput_results_%s.txt".format(taskDurationMillis)))
      (0 to 100).foreach { x =>
        val percentile = x / 100.0
        writer.write("%s\t%s\n".format(percentile, get_percentile(sortedResponseTimes, percentile)))
      }
      writer.close()
      responseTimes.clear()
      // Sleep for a bit just to make sure things are cleaned up.
      Thread.sleep(10000)
    }
  }
}

class QueryLaunchRunnable(sc: SparkContext, numTasks: Int, taskDurationMillis: Int)
  extends Runnable with Logging {
  def run() {
    val startTime = System.currentTimeMillis
    // This is a hack so that Spark doesn't try to serialize the whole QueryLaunchRunnable object.
    val millis = taskDurationMillis
    sc.parallelize(1 to numTasks, numTasks).map(x => Thread.sleep(millis)).count()
    val responseTime = System.currentTimeMillis - startTime
    logInfo("QueryTime: %s (for start time %s)".format(responseTime, startTime))
    ThroughputTester.responseTimes += responseTime.toInt
  }
}
