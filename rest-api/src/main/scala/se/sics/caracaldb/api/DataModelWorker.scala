package se.sics.caracaldb.api

import akka.actor._
import se.sics.datamodel.client.ClientManager
import se.sics.caracaldb.Key
import se.sics.caracaldb.KeyRange
import se.sics.caracaldb.operations.ResponseCode
import se.sics.caracaldb.api.data._
import se.sics.datamodel.msg.DMMessage
import java.nio.charset.Charset
import se.sics.datamodel.util.ByteId
import java.nio.ByteBuffer
import se.sics.datamodel.client.BlockingClient


// ObjModel
case class GetTypeRequest(req: String) extends CaracalRequest
case class PutTypeRequest(req: String) extends CaracalRequest
case class GetObjRequest(req: String) extends CaracalRequest
case class PutObjRequest(req: String) extends CaracalRequest
case class QueryObjRequest(req: String) extends CaracalRequest

class DataModelWorker extends Actor with ActorLogging {
	import context._
	import scala.collection.JavaConversions._
	
	private val utf8 = Charset.forName("UTF-8");
	
	val worker: BlockingClient = ClientManager.newClient();
	
	def receive = {
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
				val res: scala.collection.mutable.Map[ByteId, ByteBuffer] = resp.objs;
				sender ! Entries(res.map {
					case (k, v) => Entry(k.toString(), new String(v.array(), utf8));
				}.toList);
			} else {
				sender ! DMOperation(resp.respCode);
			}
		}
	}
}