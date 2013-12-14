package se.sics.caracaldb.api.data

import se.sics.caracaldb.operations.ResponseCode

trait CaracalResponse
case class Entry(key: String, value: String) extends CaracalResponse
case class Operation(status: ResponseCode) extends CaracalResponse
case class Entries(l: List[Entry]) extends CaracalResponse