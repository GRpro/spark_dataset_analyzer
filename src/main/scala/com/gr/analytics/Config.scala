package com.gr.analytics

import spray.json.DefaultJsonProtocol._
import spray.json._

case class Config(
                   master: Option[String],
                   sourcePath: String,
                   destPath: String,
                   csvOptions: Map[String, String],
                   conversions: List[ConverterConfig]
                 )

object ArgParser {

  private implicit val dataTypeFormat = new RootJsonFormat[ConverterConfig] {
    override def read(json: JsValue): ConverterConfig = json.asInstanceOf[JsObject].fields("new_data_type").asInstanceOf[JsString].value match {
      case Converter.StringType => jsonFormat3(StringConverterConfig).read(json)
      case Converter.IntegerType => jsonFormat3(IntegerConverterConfig).read(json)
      case Converter.DateType => jsonFormat4(DateConverterConfig).read(json)
      case Converter.BooleanType => jsonFormat3(BooleanConverterConfig).read(json)
      case tp => throw new IllegalArgumentException(s"unsupported converter type [$tp]")
    }

    override def write(obj: ConverterConfig): JsValue = ??? // not required
  }

  private implicit val configFormat = jsonFormat5(Config)

  def config(json: String): Config =
    JsonParser.apply(ParserInput.apply(json)).convertTo[Config]
}