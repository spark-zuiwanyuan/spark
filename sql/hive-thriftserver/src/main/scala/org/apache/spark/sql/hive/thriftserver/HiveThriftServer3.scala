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

package org.apache.spark.sql.hive.thriftserver

import scala.collection.JavaConversions._

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.server.{HiveServer2, ServerOptionsProcessor}

import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._

import akka.actor._
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, Utils}

import org.apache.spark.SecurityManager

import org.apache.hadoop.fs.Path


/**
 * The main entry point for the Spark SQL port of HiveServer2.  Starts up a `SparkSQLContext` and a
 * `HiveThriftServer2` thrift server.
 */
private[hive] object HiveThriftServer3 extends Logging {

  def main(args: Array[String]) {
    val optionsProcessor = new ServerOptionsProcessor("HiveThriftServer2")

    if (!optionsProcessor.process(args)) {
      System.exit(-1)
    }

    val ss = new SessionState(new HiveConf(classOf[SessionState]))

    // Set all properties specified via command line.
    val hiveConf: HiveConf = ss.getConf
    hiveConf.getAllProperties.toSeq.sortBy(_._1).foreach { case (k, v) =>
      logDebug(s"HiveConf var: $k=$v")
    }

    SessionState.start(ss)

    logInfo("Starting SparkContext")
    SparkSQLEnv.init()
    SessionState.start(ss)

    Runtime.getRuntime.addShutdownHook(
      new Thread() {
        override def run() {
          SparkSQLEnv.stop()
        }
      }
    )

    try {
      val server = new HiveThriftServer2(SparkSQLEnv.hiveContext)
      server.init(hiveConf)
      server.start()
      
      logInfo("HiveThriftServer2 started")
      
      val conf = SparkSQLEnv.hiveContext.sparkContext.conf
      val (actorSystem, _) = AkkaUtils.createActorSystem(
        "thriftServer", Utils.localHostName(), 8090, conf, new SecurityManager(conf))
      actorSystem.actorOf(Props(classOf[HiveThriftServer3], SparkSQLEnv.hiveContext), "server3")
      actorSystem.awaitTermination()
    } catch {
      case e: Exception =>
        logError("Error starting HiveThriftServer2", e)
        System.exit(-1)
    }
  }
}

private[hive] class HiveThriftServer3(hiveContext: HiveContext) 
  extends Actor with ActorLogReceive with Logging {
  
  import HiveThriftServerMessages.RegisterTable
  import java.util.concurrent.ArrayBlockingQueue
  import scala.collection.mutable.Map
  
  val queue = new ArrayBlockingQueue[String](3)
  val map = Map[String, String]()
  val hadoopConf = hiveContext.sparkContext.hadoopConfiguration
  
  def receiveWithLogging = {
    case RegisterTable(filePath, tableName) => 
      val sqlRdd = hiveContext.parquetFile(filePath)
      sqlRdd.registerTempTable(tableName)
      gc(filePath, tableName)
  }
  
  private def gc(filePath: String, tableName: String) {
    map.get(tableName) match {
      case Some(file) =>
        if (queue.remainingCapacity() == 0){
          delCpFile(queue.take())
        }
        queue.put(file)
        map(tableName) = filePath
        
      case None =>
        map(tableName) = filePath
    }
  }
  
  override def postStop() {
    queue.foreach(delCpFile(_))
    map.foreach(itr => delCpFile(itr._2))
  }
  
  private def delCpFile(filePath: String) {
    val path = new Path(filePath)
    val fs = path.getFileSystem(hadoopConf)
    fs.delete(path, true)
  }
}
