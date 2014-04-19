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

package org.apache.spark.nosql.hbase

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkContext, LocalSparkContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HConstants, HBaseTestingUtility}
import org.apache.hadoop.hbase.client.{Scan, HTable}

class HBaseSuite
  extends FunSuite
  with LocalSparkContext
  with BeforeAndAfterAll {

  val util = new HBaseTestingUtility()

  override def beforeAll() {
    util.startMiniCluster()
  }

  override def afterAll() {
    util.shutdownMiniCluster()
  }

  test("save SequenceFile as HBase table") {
    sc = new SparkContext("local", "test1")
    val nums = sc.makeRDD(1 to 3).map(x => new Text("a" + x + " 1.0"))

    val table = "test1"
    val rowkeyType = "string"
    val cfBytes = Bytes.toBytes("cf")
    val qualBytes = Bytes.toBytes("qual0")
    val columns = List[HBaseColumn](new HBaseColumn(cfBytes, qualBytes, "float"))
    val delimiter = ' '

    util.createTable(Bytes.toBytes(table), cfBytes)
    val conf = util.getConfiguration
    val zkHost = conf.get(HConstants.ZOOKEEPER_QUORUM)
    val zkPort = conf.get(HConstants.ZOOKEEPER_CLIENT_PORT)
    val zkNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT)

    HBaseUtils.saveAsHBaseTable(nums, zkHost, zkPort, zkNode, table, rowkeyType, columns, delimiter)

    // Verify results
    val htable = new HTable(conf, table)
    val scan = new Scan()
    val rs = htable.getScanner(scan)

    var result = rs.next()
    var i = 1
    while (result != null) {
      val rowkey = Bytes.toString(result.getRow)
      assert(rowkey == "a" + i)
      val value = Bytes.toFloat(result.getValue(cfBytes, qualBytes))
      assert(value == 1.0)
      result = rs.next()
      i += 1
    }

    rs.close()
    htable.close()
  }
}