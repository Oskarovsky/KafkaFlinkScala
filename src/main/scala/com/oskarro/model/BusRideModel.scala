package com.oskarro.model

import java.sql.Timestamp

case class BusRideModel(Line: Int,
                        Lon: Double,
                        VehicleNumber: String,
                        Time: Timestamp,
                        Lat: Double,
                        Brigade: Int,
                        Speed: Double)
