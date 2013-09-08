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

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap 

import edu.berkeley.sparrow.api.SparrowFrontendClient
import edu.berkeley.sparrow.thrift.FrontendService
import edu.berkeley.sparrow.thrift.TFullTaskId
import edu.berkeley.sparrow.thrift.TPlacementPreference
import edu.berkeley.sparrow.thrift.TTaskSpec
import edu.berkeley.sparrow.thrift.TUserGroupInfo

import spark._
import spark.scheduler._
import spark.scheduler.cluster._
import spark.scheduler.cluster.SchedulingMode.SchedulingMode

private[spark] class SparrowScheduler(
  sc: SparkContext, host: String, port: String, frameworkName: String)
  extends TaskScheduler with FrontendService.Iface with Logging {
    
  private val client = new SparrowFrontendClient
  private val user = new TUserGroupInfo("sparkUser", "group", 0)
  private val taskId = new AtomicInteger
  private var listener: TaskSchedulerListener = null

  // Mapping of task ids to Tasks because we need to pass a Task back to the listener on task end.
  private val tidToTask = new HashMap[String, Task[_]]()

  private var ser = SparkEnv.get.closureSerializer.newInstance()

  // The pool is just used for the UI -- so fine if we don't set it.
  val schedulingMode: SchedulingMode = SchedulingMode.withName("FIFO")
  var rootPool = new Pool("", schedulingMode, 0, 0)
  
  override def submitTasks(taskSet: TaskSet) = synchronized { 
    val sparrowTasks = taskSet.tasks.map(t => {
      val spec = new TTaskSpec
      spec.setPreference{
        val placement = new TPlacementPreference
        t.preferredLocations.foreach(p => placement.addToNodes(p.host))
        placement
      }
      spec.setMessage(Task.serializeWithDependencies(t, sc.addedFiles, sc.addedJars, ser))
      val tid = taskId.incrementAndGet().toString()
      tidToTask(tid) = t
      spec.setTaskId(tid)
      spec
    })

    val description = {
      if (taskSet.properties == null) {
        ""
      } else {
        val sparkJobDescription = taskSet.properties.getProperty(
          SparkContext.SPARK_JOB_DESCRIPTION, "").replace("\n", " ")
        "%s-%s".format(sparkJobDescription, taskSet.stageId)
      }
    }

    new Thread(new Runnable() {
      override def run() {
        client.submitJob(frameworkName, sparrowTasks.toList, user, description)
        logInfo("Submitted taskSet with id=%s time=%s".format(taskSet.id, System.currentTimeMillis))
      }
    }).start()
  }
  
  override def start() {
    val socketAddress = new InetSocketAddress(host, port.toInt)
    client.initialize(socketAddress, frameworkName, this)
  }
  
  override def stop() {
    // Do nothing.
  }

  override def defaultParallelism() = System.getProperty("spark.default.parallelism", "8").toInt

  override def setListener(listener: TaskSchedulerListener) {
    this.listener = listener
  }

  // TODO: have taskStarted message from Sparrow; on receiving that call taskStarted
  // on task listener.

  override def frontendMessage(
    taskId: TFullTaskId, statusInt: Int, message: ByteBuffer) = synchronized {
    TaskState.apply(statusInt) match {
      case TaskState.FINISHED =>
        val result = ser.deserialize[TaskResult[_]](message, getClass.getClassLoader)
        result.metrics.resultSize = message.limit() 
        
        // TODO: get this information from Sparrow, rather than fudging it.
        val taskInfo = new TaskInfo(
          taskId.getTaskId.toLong,
          0,
          System.currentTimeMillis,
          "dummyexec",
          "foo:bar",
          TaskLocality.PROCESS_LOCAL)
        listener.taskEnded(
          tidToTask(taskId.getTaskId()),
          Success,
          result.value,
          result.accumUpdates,
          taskInfo,
          result.metrics)

      case status =>
        // TODO: the fact that we don't support task started right now causes UI problems.
        logWarning("Got unexpected task state: " + status)
    }
  }
}
