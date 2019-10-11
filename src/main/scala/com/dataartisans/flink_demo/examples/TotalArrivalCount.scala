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

package com.dataartisans.flink_demo.examples

import com.dataartisans.flink_demo.datatypes.{GeoPoint, TaxiRide}
import com.dataartisans.flink_demo.sinks.ElasticsearchUpsertSink
import com.dataartisans.flink_demo.sources.TaxiRideSource
import com.dataartisans.flink_demo.utils.NycGeoUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * Apache Flink DataStream API demo application.
  *
  * The program processes a stream of taxi ride events from the New York City Taxi and Limousine
  * Commission (TLC).
  * It computes for each location the total number of persons that arrived by taxi.
  *
  * See
  * http://github.com/dataartisans/flink-streaming-demo
  * for more detail.
  *
  */

import org.apache.flink.api.scala._

object TotalArrivalCount {

  def func: ((Int, Long, Int), (Int, Long, Short)) => (Int, Long, Int)
    = (s: (Int, Long, Int), r: (Int, Long, Short)) => (s._1, s._2.max(r._2), s._3 + r._3)

  def main(args: Array[String]) {

    // input parameters
    val data = "./data/nycTaxiData.gz"
    val maxServingDelay = 60
    val servingSpeedFactor = 600f

    // Elasticsearch parameters
    val writeToElasticsearch = true // set to true to write results to Elasticsearch
    val elasticsearchHost = "127.0.0.1" // look-up hostname in Elasticsearch log output
    val elasticsearchPort = 9300


    // set up streaming execution environment
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val env  = DemoStreamEnvironment.env

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Define the data source
    //    val rides: DataStream[TaxiRide] = env.addSource(new TaxiRideSource(data, maxServingDelay, servingSpeedFactor)).setParallelism(1).forward
    val rides: DataStream[TaxiRide] = env.addSource(new TaxiRideSource(data, maxServingDelay, servingSpeedFactor))

    //      rides
    // filter for trip end events
    val cleansedRides = rides
      .filter(!_.isStart)
      // filter for events in NYC
      .filter(r => NycGeoUtils.isInNYC(r.location))

    // map location coordinates to cell Id, timestamp, and passenger count
    val cellIds: DataStream[(Int, Long, Short)] = cleansedRides
      .map { r =>
        (NycGeoUtils.mapToGridCell(r.location), r.time.getMillis, r.passengerCnt)
      }

    val passengerCnts: DataStream[(Int, Long, Int)] = cellIds
      .keyBy(_._1)
      //  def fold[R: TypeInformation](initialValue: R)(fun: (R,T) => R): DataStream[R] = {
      .fold(0, 0L, 9)(func)

    passengerCnts.filter(_._1 != 0).print()


    // sum passengers per cell Id and update time

    // map cell Id back to GeoPoint
    //    val cntByLocation: DataStream[(Int, Long, GeoPoint, Int)] = passengerCnts
    //      .map( r => (r._1, r._2, NycGeoUtils.getGridCellCenter(r._1), r._3 ) )

    // print to console
    //    cntByLocation
    //      .print()

    if (writeToElasticsearch) {
      println("example ... ...")
      // write to Elasticsearch
      //      cntByLocation
      //        .addSink(new CntTimeByLocUpsert(elasticsearchHost, elasticsearchPort))
    }

    env.execute("Total passenger count per location")

  }

  class CntTimeByLocUpsert(host: String, port: Int)
    extends ElasticsearchUpsertSink[(Int, Long, GeoPoint, Int)](
      host,
      port,
      "elasticsearch",
      "nyc-idx",
      "popular-locations") {

    override def insertJson(r: (Int, Long, GeoPoint, Int)): Map[String, AnyRef] = {
      Map(
        "location" -> (r._3.lat + "," + r._3.lon).asInstanceOf[AnyRef],
        "time" -> r._2.asInstanceOf[AnyRef],
        "cnt" -> r._4.asInstanceOf[AnyRef]
      )
    }

    override def updateJson(r: (Int, Long, GeoPoint, Int)): Map[String, AnyRef] = {
      Map[String, AnyRef](
        "time" -> r._2.asInstanceOf[AnyRef],
        "cnt" -> r._4.asInstanceOf[AnyRef]
      )
    }

    override def indexKey(r: (Int, Long, GeoPoint, Int)): String = {
      // index by location
      r._1.toString
    }
  }

}

class SumAggFunction extends KeyedProcessFunction[Int, (Int, Long, Short), (Int, Long, Short)] {
  override def processElement(value: (Int, Long, Short), ctx: KeyedProcessFunction[Int, (Int, Long, Short), (Int, Long, Short)]#Context, out: Collector[(Int, Long, Short)]): Unit = ???
}
