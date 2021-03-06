/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink_demo.sinks

import java.net.InetAddress

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * SinkFunction to either insert or update an entry in an Elasticsearch index.
  *
  * @param host    Hostname of the Elasticsearch instance.
  * @param port    Port of the Elasticsearch instance.
  * @param cluster Name of the Elasticsearch cluster.
  * @param index   Name of the Elasticsearch index.
  * @param mapping Name of the index mapping.
  * @tparam T Record type to write to Elasticsearch.
  */
abstract class ElasticsearchUpsertSink[T](host: String, port: Int, cluster: String, index: String, mapping: String)
  extends RichSinkFunction[T] {

  private var client: TransportClient = null

  def insertJson(record: T): Map[String, AnyRef]

  def updateJson(record: T): Map[String, AnyRef]

  def indexKey(record: T): String

  private val log = LogManager.getLogger(getClass.getName)

  @throws[Exception]
  override def open(parameters: Configuration) {

    val setting = Settings.builder().put("cluster.name", cluster).build()

    client = new PreBuiltTransportClient(setting)
      .addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300))

  }

  @throws[Exception]
  override def invoke(r: T) {
    // do an upsert request to elastic search

    // index document if it does not exist
    Try {
      val indexRequest = new IndexRequest(index, mapping, indexKey(r))
        .source(mapAsJavaMap(insertJson(r)))

      // update document if it exists
      val updateRequest = new UpdateRequest(index, mapping, indexKey(r))
        .doc(mapAsJavaMap(updateJson(r)))
        .upsert(indexRequest)

      client.update(updateRequest).get()

    } match {
      case scala.util.Failure(exception) => log.error(exception.getMessage)
      case scala.util.Success(value) => log.info(value)
    }
  }
}
