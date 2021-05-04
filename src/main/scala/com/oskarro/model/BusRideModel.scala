package com.oskarro.model

import java.sql.Timestamp

case class BusRideModel(Lines: String,
                        Lon: Double,
                        VehicleNumber: String,
                        Time: String,
                        Lat: Double,
                        Brigade: String,
                        Speed: Double = 1.11)
