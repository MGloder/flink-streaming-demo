package com.dataartisans.flink_demo.examples

import com.dataartisans.flink_demo.datatypes.{GeoPoint, TaxiRide}
import com.dataartisans.flink_demo.sources.TaxiRideSource
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object PulsarExample {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val taxiSource = env
      .addSource(
        new TaxiRideSource("/Users/xyan/Learning/flink-streaming-demo/data/nycTaxiData.gz",
          10, 10.0f))

    val valueStateIntermediateResult = taxiSource
      .keyBy(taxi => taxi.rideId)
      .process(new ValueStateProcessFunctionExample)

    val customTaxiRideStringSink: StreamingFileSink[TaxiRide] = StreamingFileSink.forRowFormat(
      new Path("/Users/xyan/Learning/flink-streaming-demo/data/result.gz"),
      new SimpleStringEncoder[TaxiRide]("UTF-8")).build()

//    valueStateIntermediateResult.addSink(StreamingFileSink.forRowFormat[TaxiRide](
//
//    ))

    //    taxiSource.("/Users/xyan/Learning/flink-streaming-demo/data/result.gz", WriteMode.OVERWRITE)
    valueStateIntermediateResult.addSink(customTaxiRideStringSink)

    env.execute("Taxi Source with Multiple Managed State Test")
  }
}

case class ValueStateProcessFunctionExample() extends KeyedProcessFunction[Long, TaxiRide, TaxiRide] {
  private var lastGeoLocation: ValueState[GeoPoint] = _

  override def processElement(value: TaxiRide,
                              ctx: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#Context,
                              out: Collector[TaxiRide]): Unit = {
    out.collect(value)
    if (lastGeoLocation != null) {
      println("last geo is: " +
        this.lastGeoLocation.value() + " current geo is: " +
        value.location + " marching " + (value.location - this.lastGeoLocation.value()))
    }
    this.lastGeoLocation.update(value.location)
  }

  override def open(parameters: Configuration): Unit = {
    val lastGeoDescriptor = new ValueStateDescriptor[GeoPoint]("GeoPoint", classOf[GeoPoint],
      new GeoPoint(0.0f, 0.0f))
    lastGeoLocation = getRuntimeContext.getState[GeoPoint](lastGeoDescriptor)
  }
}


