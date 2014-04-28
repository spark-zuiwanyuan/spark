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

import org.apache.hadoop.hbase.client.{HConnectionManager, Put, HTable}
import org.apache.hadoop.io.Text
import org.apache.hadoop.hbase.util.Bytes
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HBaseConfiguration
import java.io.IOException
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.types._
import scala.Some
import org.apache.spark.sql.Row

/**
 * Internal helper class that saves an RDD using a HBase OutputFormat. This is only public
 * because we need to access this class from the `hbase` package to use some package-private HBase
 * functions, but this class should not be used directly by users.
 */
private[hbase]
class SparkHBaseWriter(conf: HBaseConf) extends Logging {

  private var htable: HTable = null

  val zkHost = conf.zkHost
  val zkPort = conf.zkPort
  val zkNode = conf.zkNode
  val table = conf.table
  val rowkeyType = conf.rowkeyType
  val columns = conf.columns
  val delimiter = conf.delimiter

  def init() {
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkHost)
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, zkPort)
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkNode)
    htable = new HTable(conf, table)
    // Use default writebuffersize to submit batch puts
    htable.setAutoFlush(false)
  }

  /**
   * Convert field to bytes
   * @param field split by delimiter from record
   * @param kind the type of field
   * @return
   */
  def toByteArray(field: String, kind: String) = kind match {
    case HBaseType.Boolean => Bytes.toBytes(field.toBoolean)
    case HBaseType.Short => Bytes.toBytes(field.toShort)
    case HBaseType.Int => Bytes.toBytes(field.toInt)
    case HBaseType.Long => Bytes.toBytes(field.toLong)
    case HBaseType.Float => Bytes.toBytes(field.toFloat)
    case HBaseType.Double => Bytes.toBytes(field.toDouble)
    case HBaseType.String => Bytes.toBytes(field)
    case HBaseType.Bytes => Hex.decodeHex(field.toCharArray)
    case _ => throw new IOException("Unsupported data type.")
  }

  /**
   * Convert a [[org.apache.hadoop.io.Text]] record to [[org.apache.hadoop.hbase.client.Put]]
   * and put it to HBase
   * @param record
   * @return
   */
  def write(record: Text) = {
    val fields = record.toString.split(delimiter)
    
    if (columns.isEmpty)
    {
      logWarning(s"collumns is unacceptable.")
    } else{
      // Check the format of record
      val k = columns.length
      if (fields.size > columns.length) {
        //insert the null
        List.range(fields.size, k) foreach {
          i => {
            val b = columns.last.qualifier
            b.update(b.length-1, (b.last+1).toByte)
            columns :+ new HBaseColumn(columns.last.family, b, columns.last.typ)
          }
        }
      }
    
      val put = new Put(columns(0).family)

      List.range(0, fields.size) foreach {
        i => put.add(columns(i).family, columns(i).qualifier,
          toByteArray(fields(i), columns(i).typ))
      }

      htable.put(put)
    }

  }
  
  /**
   * Convert a [[org.apache.spark.sql.Row]] record to [[org.apache.hadoop.hbase.client.Put]]
   * and put it to HBase
   * @param record
   * @return
   */
  def write(record: Row) = {
    // Check the format of record
	if (record.length == columns.length + 1) {
	  val put = new Put(toByteArray(record, 0, rowkeyType))
	  List.range(1, record.length) foreach {
	    i => put.add(columns(i - 1).family, columns(i - 1).qualifier,
	    toByteArray(record, i, columns(i - 1).dataType))
	  }
	  htable.put(put)
	} else {
	  logWarning(s"Record ($record) is unacceptable.")
	}
  }

  def close() {
    Option(htable) match {
      case Some(t) => t.close()
      case None => logWarning("HBase table variable is null!")
    }
  }
}

/**
 * Contains the types which supported to
 * parse from [[java.lang.String]] into [[scala.Array[Byte]]]
 */
object HBaseType {
  val Boolean = "bool"
  val Short = "short"
  val Int = "int"
  val Long = "long"
  val Float = "float"
  val Double = "double"
  val String = "string"
  val Bytes = "bytes"
}

/**
 * A representation of HBase Column
 * @param family HBase Column Family
 * @param qualifier HBase Column Qualifier
 * @param typ type [[org.apache.spark.nosql.hbase.HBaseType]]
 */
class HBaseColumn(val family: Array[Byte], val qualifier: Array[Byte], val typ: String)
  extends Serializable

/**
 * A representation of HBase Configuration.
 * It contains important parameters which used to create connection to HBase Servers
 * @param zkHost the zookeeper hosts. e.g. "10.232.98.10,10.232.98.11,10.232.98.12"
 * @param zkPort the zookeeper client listening port. e.g. "2181"
 * @param zkNode the zookeeper znode of HBase. e.g. "hbase-apache"
 * @param table the name of table which we save records
 * @param rowkeyType the type of rowkey. [[org.apache.spark.nosql.hbase.HBaseType]]
 * @param columns the column list. [[org.apache.spark.nosql.hbase.HBaseColumn]]
 * @param delimiter the delimiter which used to split record into fields
 */
class HBaseConf(val zkHost: String, val zkPort: String, val zkNode: String,
                val table: String, val rowkeyType: String, val columns: List[HBaseColumn],
                val delimiter: Char)
  extends Serializable