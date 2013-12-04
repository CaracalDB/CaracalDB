package se.sics.caracaldb.api.data

import se.sics.caracaldb.operations.ResponseCode

case class Entry(key: String, value: String)
case class Operation(status: ResponseCode)