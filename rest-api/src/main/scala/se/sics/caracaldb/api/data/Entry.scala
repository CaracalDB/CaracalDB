package se.sics.caracaldb.api.data

import se.sics.caracaldb.operations.ResponseCode
import se.sics.datamodel.msg.DMMessage

trait CaracalResponse
case class Entry(key: String, value: String) extends CaracalResponse
case class Operation(status: ResponseCode) extends CaracalResponse
case class DMOperation(status: DMMessage.ResponseCode) extends CaracalResponse
case class FormattedResponse(str: String) extends CaracalResponse
case class Entries(l: List[Entry]) extends CaracalResponse