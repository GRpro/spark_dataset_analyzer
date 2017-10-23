package com.gr.analytics

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, to_date}

sealed trait ConverterConfig

case class StringConverterConfig(existing_col_name: String, new_col_name: String, new_data_type: String) extends ConverterConfig

case class IntegerConverterConfig(existing_col_name: String, new_col_name: String, new_data_type: String) extends ConverterConfig

case class DateConverterConfig(existing_col_name: String, new_col_name: String, new_data_type: String, date_expression: String) extends ConverterConfig

case class BooleanConverterConfig(existing_col_name: String, new_col_name: String, new_data_type: String) extends ConverterConfig


object Converter {
  final val StringType = "string"
  final val IntegerType = "integer"
  final val DateType = "date"
  final val BooleanType = "boolean"

  def convert[T <: ConverterConfig](converterConfig: T): Column = converterConfig match {
    case converter: StringConverterConfig =>
      col(converter.existing_col_name)
        .as(converter.new_col_name)
        .cast(converter.new_data_type)

    case converter: IntegerConverterConfig =>
      col(converter.existing_col_name)
        .as(converter.new_col_name)
        .cast(converter.new_data_type)

    case converter: DateConverterConfig =>
      to_date(col(converter.existing_col_name), converter.date_expression).as(converter.new_col_name)

    case converter: BooleanConverterConfig =>
      col(converter.existing_col_name)
        .as(converter.new_col_name)
        .cast(converter.new_data_type)
  }
}
