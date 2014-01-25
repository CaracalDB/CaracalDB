package se.sics.caracaldb.api

import akka.actor._
import se.sics.caracaldb.client.ClientManager
import se.sics.caracaldb.Key
import se.sics.caracaldb.KeyRange
import se.sics.caracaldb.operations.ResponseCode
import se.sics.caracaldb.api.data._
import se.sics.caracaldb.datamodel.msg.DMMessage
import java.nio.charset.Charset
import se.sics.caracaldb.datamodel.util.ByteId

trait CaracalRequest
case class GetRequest(key: Key) extends CaracalRequest
case class PutRequest(key: Key, value: Array[Byte]) extends CaracalRequest
case class RangeRequest(range: KeyRange) extends CaracalRequest
// ObjModel
case class GetTypeRequest(req: String) extends CaracalRequest
case class PutTypeRequest(req: String) extends CaracalRequest
case class GetObjRequest(req: String) extends CaracalRequest
case class PutObjRequest(req: String) extends CaracalRequest
case class QueryObjRequest(req: String) extends CaracalRequest

class CaracalWorker extends Actor with ActorLogging {
	import context._
	import scala.collection.JavaConversions._
	
	private val utf8 = Charset.forName("UTF-8");
	
	val worker = ClientManager.newClient();
	
	def receive = {
		case GetRequest(key) => {
			log.debug("GET {}", key);
			val resp = worker.get(key);
			log.debug("GET response: {}", resp);
			if (resp.code == ResponseCode.SUCCESS) {
				sender ! Entry(resp.key.toString(), new String(resp.data, utf8));
			} else {
				sender ! Operation(resp.code);
			}
		}
		case PutRequest(key, value) => {
			log.debug("PUT ({}, {})", key, value);
			val resp = worker.put(key, value);
			log.debug("PUT response: {}", resp);
			sender ! Operation(resp);
		}
		case RangeRequest(range) => {
			log.debug("Range {}", range);
			val resp = worker.rangeRequest(range);
			if (resp.code == ResponseCode.SUCCESS) {
				val res: scala.collection.mutable.Map[Key, Array[Byte]] = resp.results;
				sender ! Entries(res.map {
					case (k, v) => Entry(k.toString(), new String(v, utf8));
				}.toList);
			} else {
				sender ! Operation(resp.code);
			}
		}
		case GetTypeRequest(req) => {
			log.debug("GetType {}", req);
			val resp = worker.getType(req);
			if (resp.respCode == DMMessage.ResponseCode.SUCCESS) {
				val res = new String(resp.typeInfo, utf8);
				sender ! FormattedResponse(res);
			} else {
				sender ! DMOperation(resp.respCode);
			}
		}
		case PutTypeRequest(req) => {
			log.debug("PutType {}", req);
			val resp = worker.putType(req);
			sender ! DMOperation(resp);
		}
		case GetObjRequest(req) => {
			log.debug("GetObj {}", req);
			val resp = worker.getObj(req);
			if (resp.respCode == DMMessage.ResponseCode.SUCCESS) {
				val res = new String(resp.value, utf8);
				sender ! FormattedResponse(res);
			} else {
				sender ! DMOperation(resp.respCode);
			}
		}
		case PutObjRequest(req) => {
			log.debug("PutObj {}", req);
			val resp = worker.putObj(req);
			sender ! DMOperation(resp);
		}
		case QueryObjRequest(req) => {
			log.debug("QueryObj {}", req);
			val resp = worker.queryObj(req);
			if (resp.respCode == DMMessage.ResponseCode.SUCCESS) {
				val res: scala.collection.mutable.Map[ByteId, Array[Byte]] = resp.objs;
				sender ! Entries(res.map {
					case (k, v) => Entry(k.toString(), new String(v, utf8));
				}.toList);
			} else {
				sender ! DMOperation(resp.respCode);
			}
		}
	}
}