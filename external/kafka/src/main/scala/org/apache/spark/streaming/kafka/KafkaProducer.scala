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

package org.apache.spark.streaming.kafka

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import org.apache.spark.Logging

private[streaming]
class KafaProducer(brStr: String) extends Logging{
  private var prod: Producer[String, String] = null
  
  def init(){
    val props = new Properties()
    props.put("metadata.broker.list", brStr)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    prod = new Producer[String, String](config)
  }
  
  def produce(msg: String)
  {
    val data = new KeyedMessage[String, String]("log-error", "error", msg)
    prod.send(data)
  }
  
  def close() {
    Option(prod) match {
      case Some(t) => t.close
      case None => logWarning("HBase table variable is null!")
    }
  }
}