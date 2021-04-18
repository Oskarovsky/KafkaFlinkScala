package com.oskarro.services

import com.oskarro.model.BusModel
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

/**
 * CassandraService is a class that sinks DataStream into CassandraDB.
 */

class CassandraService {

  /**
   * Creating environment for Cassandra and sink some Data of car stream into CassandraDB
   *
   * @param busDataStream DataStream of type Car.
   */
  def sinkToCassandraDB(busDataStream: DataStream[BusModel]): Unit = {

    createTypeInformation[(String, Long, String, Long, String, Long, String)]

    // Creating bus data to sink into cassandraDB.
    val sinkBusDataStream = busDataStream
      .map(bus => (bus.Lines, bus.Lon, bus.VehicleNumber, bus.Time, bus.Lat, bus.Brigade))

    CassandraSink.addSink(sinkBusDataStream)
      .setHost("127.0.0.1")
      .setQuery("INSERT INTO stuff.bus_flink(" +
        "\"Uuid\", " +
        "\"Lines\", " +
        "\"Lon\", " +
        "\"VehicleNumber\", " +
        "\"Time\", " +
        "\"Lat\", " +
        "\"Brigade\")" +
        " values (?, ?, ?, ?, ?, ?, ?);")
      .build()

  }
}
