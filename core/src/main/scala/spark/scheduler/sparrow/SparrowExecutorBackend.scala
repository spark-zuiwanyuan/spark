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
import java.util.ArrayList

import scala.collection.mutable.HashMap

import org.apache.thrift.async.AsyncMethodCallback

import edu.berkeley.sparrow.daemon.util.TClients
import edu.berkeley.sparrow.daemon.util.ThriftClientPool
import edu.berkeley.sparrow.daemon.util.TServers
import edu.berkeley.sparrow.thrift.BackendService
import edu.berkeley.sparrow.thrift.NodeMonitorService
import edu.berkeley.sparrow.thrift.NodeMonitorService.AsyncClient
import edu.berkeley.sparrow.thrift.NodeMonitorService.AsyncClient.sendFrontendMessage_call
import edu.berkeley.sparrow.thrift.NodeMonitorService.AsyncClient.tasksFinished_call
import edu.berkeley.sparrow.thrift.TFullTaskId
import edu.berkeley.sparrow.thrift.TUserGroupInfo

import spark.executor.Executor
import spark.executor.ExecutorBackend
import spark.Logging
import spark.TaskState
import spark.TaskState.TaskState
import spark.Utils

/**
 * A Sparrow version of an {@link ExecutorBackend}.
 *
 * Acts as a Sparrow backend and listens for task launch requests from Sparrow. Also passes
 * statusUpdate messages from a Spark executor back to Sparrow.
 */
class SparrowExecutorBackend extends ExecutorBackend with Logging with BackendService.Iface {
  private val executor = new Executor(
    "SparrowExecutor_%s".format(Utils.localHostName), Utils.localHostName, List[(String, String)]())

  // TODO: Make this configurable.
  private val nodeMonitorAddress = new InetSocketAddress("localhost", 20501)
  private val appName = System.getProperty("sparrow.app.name", "spark")

  private val taskIdToFullTaskId = new HashMap[Long, TFullTaskId]()
  private val clientPool = new ThriftClientPool[NodeMonitorService.AsyncClient](
    new ThriftClientPool.NodeMonitorServiceMakerFactory())

  /** Callback to use for asynchronous Thrift function calls. */
  class ThriftCallback[T](client: AsyncClient) extends AsyncMethodCallback[T] {
    def onComplete(response: T) {
      try {
        clientPool.returnClient(nodeMonitorAddress, client)
      } catch {
        case e: Exception => e.printStackTrace(System.err)
      }
    }

    def onError(exception: Exception) {
      exception.printStackTrace(System.err)
    }
  }

  def initialize() {
    val client = TClients.createBlockingNmClient(
      nodeMonitorAddress.getHostName(), nodeMonitorAddress.getPort())
    client.registerBackend(appName, "localhost:" + SparrowExecutorBackend.listenPort)
  }

  override def launchTask(
    message: ByteBuffer, taskId: TFullTaskId, user: TUserGroupInfo) = synchronized {
    val taskIdLong = taskId.taskId.toLong
    taskIdToFullTaskId(taskIdLong) = taskId
    executor.launchTask(this, taskIdLong, message)
  }

  override def statusUpdate(
    taskId: Long, state: TaskState.TaskState, data: ByteBuffer): Unit = synchronized {
    if (state == TaskState.RUNNING) {
      // Ignore running messages, which just generate extra traffic.
      return
    }

    val fullId = taskIdToFullTaskId(taskId)
    
    if (state == TaskState.FINISHED) {
      val client = clientPool.borrowClient(nodeMonitorAddress)
      val finishedTasksList = new ArrayList[TFullTaskId]()
      finishedTasksList.add(fullId)
      client.tasksFinished(finishedTasksList, new ThriftCallback[tasksFinished_call](client))
    }

    // Use a new client here because asynchronous clients can only be used for one function call
    // at a time.
    val client = clientPool.borrowClient(nodeMonitorAddress)
    client.sendFrontendMessage(
      appName, fullId, state.id, data, new ThriftCallback[sendFrontendMessage_call](client))
  }
}

object SparrowExecutorBackend {
  var listenPort = 33333
  
  def run() {
    val backend = new SparrowExecutorBackend
    val processor = new BackendService.Processor[BackendService.Iface](backend)
    var foundPort = false
    while (!foundPort) {
      try {
        TServers.launchThreadedThriftServer(listenPort, 4, processor)
        foundPort = true
      } catch {
        case e: java.io.IOException =>
          println("Failed to listen on port %d; trying next port ".format(listenPort))
          listenPort = listenPort + 1
      }
    }
    backend.initialize()
  }

  def main(args: Array[String]) {
    if (args.length != 0) {
      System.err.println("Usage: SparrowExecutorBackend")
      System.exit(1)
    }
    run()
  }
}

