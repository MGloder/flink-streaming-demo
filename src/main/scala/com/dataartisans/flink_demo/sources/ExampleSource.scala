package com.dataartisans.flink_demo.sources

import org.apache.flink.streaming.api.functions.source.SourceFunction

class ExampleSource
  extends SourceFunction[String]{
  override def run(ctx: SourceFunction.SourceContext[String]): Unit =
    ctx.collect("1")

  override def cancel(): Unit = false
}
