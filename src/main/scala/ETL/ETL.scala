package ETL

import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{DataFrame, functions => F}

object ETL {

  def flattenLeg(df: DataFrame): DataFrame = {
    df
      .withColumn("Leg", F.explode(F.col("event.SlsValidatedNotificationEvent.Flight.Leg")))
      .select(
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftOwner").as("AircraftOwner"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftRegistration").as("AircraftRegistration"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftSubType").as("AircraftSubType"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftType").as("AircraftType"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.DetailCode").as("DetailCode"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.VersionCode").as("VersionCode"),
        col("event.SlsValidatedNotificationEvent.Flight.AirlDsgCd").as("AirlDsgCd"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.ActOUTDt").as("ActOUTDt"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.ActOUTDtLocal").as("ActOUTDtLocal_Leg"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.Direction").as("Direction_Leg"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.Route").as("Route_Leg"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.SchOUTDt").as("SchOUTDt_Leg"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.SchOUTDtLocal").as("SchOUTDtLocal_Leg"),
        col("event.SlsValidatedNotificationEvent.Flight.FltIdDt").as("FltIdDt"),
        col("event.SlsValidatedNotificationEvent.Flight.FltNbr").as("FltNbr"),
        concat_ws(", ", col("Leg.Cargo.BlockedSpaceAgreement")).as("Leg_Cargo_BlockedSpaceAgreement"),
        concat_ws(", ", col("Leg.Cargo.BlockedSpaceOccupied")).as("Leg_Cargo_BlockedSpaceOccupied"),
        concat_ws(", ", col("Leg.Cargo.Cargo")).as("Leg_Cargo_Cargo"),
        concat_ws(", ", col("Leg.Cargo.Mail")).as("Leg_Cargo_Mail"),
        concat_ws(", ", col("Leg.Cargo.NonRevenueCargo")).as("Leg_Cargo_NonRevenueCargo"),
        concat_ws(", ", col("Leg.Cargo.Pallets")).as("Leg_Cargo_Pallets"),
        concat_ws(", ", col("Leg.Cargo.RealizedWeightCargo")).as("Leg_Cargo_RealizedWeightCargo"),
        concat_ws(", ", col("Leg.Crew.CrewClass.CabinClass")).as("Leg_Crew_CrewClass_CabinClass"),
        concat_ws(", ", col("Leg.Crew.CrewClass.XCM")).as("Leg_Crew_CrewClass_XCM"),
        concat_ws(", ", col("Leg.Crew.CrewClass.XFA")).as("Leg_Crew_CrewClass_XFA"),
        concat_ws(", ", col("Leg.Crew.XXDHC")).as("Leg_Crew_CrewClass_XXDHC"),
        col("Leg.DepartureSequenceNumber").as("Leg_Crew_DepartureSequenceNumber"),
        col("Leg.LegActOUTDt").as("Leg_LegActOUTDt"),
        col("Leg.LegDirection").as("Leg_LegDirection"),
        col("Leg.LegSchOUTDt").as("Leg_LegSchOUTDt"),
        concat_ws(", ", col("Leg.Pax.Downgrades")).as("Leg_Pax_Downgrades"),
        concat_ws(", ", col("Leg.Pax.ElectronicCoupons")).as("Leg_Pax_ElectronicCoupons"),
        concat_ws(", ", col("Leg.Pax.NonRevenue")).as("Leg_Pax_NonRevenue"),
        concat_ws(", ", col("Leg.Pax.NonRevenueBabies")).as("Leg_Pax_NonRevenueBabies"),
        concat_ws(", ", col("Leg.Pax.Upgrades")).as("Leg_Pax_Upgrades"),
        col("Leg.SchArrStn").as("Leg_SchArrStn"),
        col("Leg.SchDepStn").as("Leg_SchDepStn"),
        col("event.SlsValidatedNotificationEvent.NotificationTimeStamp").as("NotificationTimeStamp"),
        col("event.SlsValidatedNotificationEvent.Remark").as("Remark"),
        col("event.SlsValidatedNotificationEvent.SlsEventCode").as("SlsEventCode"),
        col("timestamp")
      )
  }

  def flattenSegment(df: DataFrame): DataFrame = {
    df
      .withColumn("Segment", F.explode(F.col("event.SlsValidatedNotificationEvent.Flight.Segment")))
      .select(
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftOwner").as("AircraftOwner"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftRegistration").as("AircraftRegistration"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftSubType").as("AircraftSubType"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.AircraftType").as("AircraftType"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.DetailCode").as("DetailCode"),
        col("event.SlsValidatedNotificationEvent.Flight.Aircraft.VersionCode").as("VersionCode"),
        col("event.SlsValidatedNotificationEvent.Flight.AirlDsgCd").as("AirlDsgCd"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.ActOUTDt").as("ActOUTDt"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.ActOUTDtLocal").as("ActOUTDtLocal_Segment"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.Direction").as("Direction_Segment"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.Route").as("Route_Segment"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.SchOUTDt").as("SchOUTDt_Segment"),
        col("event.SlsValidatedNotificationEvent.Flight.FlightInfo.SchOUTDtLocal").as("SchOUTDtLocal_Segment"),
        col("event.SlsValidatedNotificationEvent.Flight.FltIdDt").as("FltIdDt"),
        col("event.SlsValidatedNotificationEvent.Flight.FltNbr").as("FltNbr"),
        col("Segment.ArrivalSequenceNumber").as("Segment_ArrivalSequenceNumber"),
        concat_ws(", ", col("Segment.Cargo.BlockedSpaceAgreement")).as("Segment_Cargo_BlockedSpaceAgreement"),
        concat_ws(", ", col("Segment.Cargo.BlockedSpaceOccupied")).as("Segment_Cargo_BlockedSpaceOccupied"),
        concat_ws(", ", col("Segment.Cargo.Cargo")).as("Segment_Cargo_Cargo"),
        concat_ws(", ", col("Segment.Cargo.Mail")).as("Segment_Cargo_Mail"),
        concat_ws(", ", col("Segment.Cargo.NonRevenueCargo")).as("Segment_Cargo_NonRevenueCargo"),
        concat_ws(", ", col("Segment.Cargo.Pallets")).as("Segment_Cargo_Pallets"),
        concat_ws(", ", col("Segment.Cargo.RealizedWeightCargo")).as("Segment_Cargo_RealizedWeightCargo"),
        concat_ws(", ", col("Segment.Crew.CrewClass.CabinClass")).as("Segment_Crew_CrewClass_CabinClass"),
        concat_ws(", ", col("Segment.Crew.CrewClass.XCM")).as("Segment_Crew_CrewClass_XCM"),
        concat_ws(", ", col("Segment.Crew.CrewClass.XFA")).as("Segment_Crew_CrewClass_XFA"),
        concat_ws(", ", col("Segment.Crew.XXDHC")).as("Segment_Crew_CrewClass_XXDHC"),
        col("Segment.DepartureSequenceNumber").as("Segment_Crew_DepartureSequenceNumber"),
        col("Segment.SchArrStn").as("Segment_SchArrStn"),
        col("Segment.SchDepStn").as("Segment_SchDepStn"),
        col("Segment.SegmentDirection").as("Segment_SegmentDirection"),
        col("event.SlsValidatedNotificationEvent.NotificationTimeStamp").as("NotificationTimeStamp"),
        col("event.SlsValidatedNotificationEvent.Remark").as("Remark"),
        col("event.SlsValidatedNotificationEvent.SlsEventCode").as("SlsEventCode"),
        col("timestamp")
      )
  }


  def joinDataframes(legDF: DataFrame, segmentDF: DataFrame): DataFrame = {
    legDF.join(segmentDF,
      Seq("AircraftOwner", "AircraftRegistration", "AircraftSubType", "AircraftType", "DetailCode", "VersionCode", "AirlDsgCd", "ActOUTDt", "FltIdDt", "FltNbr", "NotificationTimeStamp", "Remark", "SlsEventCode", "timestamp"),
      "outer"
    )
  }
}
