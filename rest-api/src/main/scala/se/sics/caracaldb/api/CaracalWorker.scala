package se.sics.caracaldb.api

import akka.actor._
import se.sics.caracaldb.client.ClientManager
import se.sics.caracaldb.Key
import se.sics.caracaldb.operations.ResponseCode
import se.sics.caracaldb.api.data._
import java.nio.charset.Charset

trait CaracalRequest
case class GetRequest(key: Key) extends CaracalRequest
case class PutRequest(key: Key, value: Array[Byte]) extends CaracalRequest

class CaracalWorker extends Actor with ActorLogging {
	import context._
	
	val worker = ClientManager.newClient();
	
	def receive = {
		case GetRequest(key) => {
			val resp = worker.get(key);
			if (resp.code == ResponseCode.SUCCESS) {
				sender ! Entry(resp.key.toString(), new String(resp.data, Charset.forName("UTF-8")));
			} else {
				sender ! Operation(resp.code);
			}
		}
		case PutRequest(key, value) => {
			val resp = worker.put(key, value);
			sender ! Operation(resp);
		}
	}
}