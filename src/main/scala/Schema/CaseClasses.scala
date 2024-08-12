package Schema

case class Aircraft(
                     AircraftOwner: Option[String],
                     AircraftRegistration: Option[String],
                     AircraftSubType: Option[String],
                     AircraftType: Option[String],
                     DetailCode: Option[String],
                     VersionCode: Option[String]
                   )

case class FlightInfo(
                       ActOUTDt: Option[String],
                       ActOUTDtLocal: Option[String],
                       Direction: Option[String],
                       Route: Option[String],
                       SchOUTDt: Option[String],
                       SchOUTDtLocal: Option[String]
                     )

case class PaxClass(
                     Adults: Option[Long],
                     BlockedSeatsAgreement: Option[String],
                     CabinClass: Option[String],
                     Children: Option[Long],
                     NonRevenueAdults: Option[Long],
                     NonRevenueChildren: Option[Long],
                     Seats: Option[Long]
                   )

case class Pax(
                Downgrades: Option[Long],
                ElectronicCoupons: Option[Long],
                NonRevenue: Option[Long],
                NonRevenueBabies: Option[Long],
                PaxClass: Seq[PaxClass],
                Upgrades: Option[Long]
              )

case class Cargo(
                  BlockedSpaceAgreement: Option[Long],
                  BlockedSpaceOccupied: Option[Long],
                  Cargo: Option[Long],
                  Mail: Option[Long],
                  NonRevenueCargo: Option[Long],
                  Pallets: Option[Long],
                  RealizedWeightCargo: Option[Long]
                )

case class CrewClass(
                      CabinClass: Option[String],
                      XCM: Option[Long],
                      XFA: Option[Long]
                    )

case class Crew(
                 CrewClass: Seq[CrewClass],
                 DepartureSequenceNumber: Option[Long],
                 XXDHC: Seq[Crew]
               )

case class Leg(
                Cargo: Cargo,
                Crew: Crew,
                DepartureSequenceNumber: Option[Long],
                LegActOUTDt: Option[String],
                LegDirection: Option[String],
                LegSchOUTDt: Option[String],
                Pax: Pax,
                SchArrStn: Option[String],
                SchDepStn: Option[String]
              )

case class Segment(
                    ArrivalSequenceNumber: Option[Long],
                    Cargo: Cargo,
                    Crew: Crew,
                    DepartureSequenceNumber: Option[Long],
                    Pax: Pax,
                    SchArrStn: Option[String],
                    SchDepStn: Option[String],
                    SegmentDirection: Option[String]
                  )

case class Flight(
                   Aircraft: Aircraft,
                   AirlDsgCd: Option[String],
                   FlightInfo: FlightInfo,
                   FltIdDt: Option[String],
                   FltNbr: Option[String],
                   Leg: Seq[Leg],
                   Segment: Seq[Segment]
                 )

case class SlsValidatedNotificationEvent(
                                          Flight: Flight,
                                          NotificationTimeStamp: Option[String],
                                          Remark: Option[String],
                                          SlsEventCode: Option[String]
                                        )

case class Event(
                  SlsValidatedNotificationEvent: SlsValidatedNotificationEvent
                )

case class Root(
                 event: Event,
                 timestamp: Option[String]
               )


