package se.sics.caracaldb.api.data

import spray.json._
import se.sics.caracaldb.operations.ResponseCode
import se.sics.datamodel.msg.DMMessage

object CaracalJsonProtocol extends DefaultJsonProtocol {
	import DefaultJsonProtocol._

	implicit object ResponseCodeFormat extends JsonFormat[ResponseCode] {
		def write(c: ResponseCode) = new JsString(c.name());
		def read(value: JsValue) = value match {
			case JsString(name) =>
				ResponseCode.valueOf(name);
			case _ => deserializationError("ResponseCode expected")
		}
	}
	
	implicit object ObjMResponseCodeFormat extends JsonFormat[DMMessage.ResponseCode] {
		def write(c: DMMessage.ResponseCode) = new JsString(c.name());
		def read(value: JsValue) = value match {
			case JsString(name) =>
				DMMessage.ResponseCode.valueOf(name);
			case _ => deserializationError("DMMessage.ResponseCode expected")
		}
	}
	
	implicit object FormattedFormat extends RootJsonFormat[FormattedResponse] {
		def write(c: FormattedResponse) = new JsString(c.str);
		def read(value: JsValue) = value match {
			case JsString(str) =>
				FormattedResponse(str)
			case _ => deserializationError("FormattedResponse expected")
		}
	}

	implicit val entryFormat = jsonFormat2(Entry);
	implicit val operationFormat = jsonFormat1(Operation);
	implicit val dmOperationFormat = jsonFormat1(DMOperation);
}