package se.sics.caracaldb.api

import akka.actor._
import se.sics.caracaldb.client.ClientManager
import se.sics.caracaldb.Key
import se.sics.caracaldb.KeyRange
import se.sics.caracaldb.operations.ResponseCode
import se.sics.caracaldb.api.data._
import java.nio.charset.Charset

trait CaracalRequest
case class GetRequest(key: Key) extends CaracalRequest
case class PutRequest(key: Key, value: Array[Byte]) extends CaracalRequest
case class RangeRequest(range: KeyRange) extends CaracalRequest

class CaracalWorker extends Actor with ActorLogging {
	import context._
	import scala.collection.JavaConversions._
	
	val worker = ClientManager.newClient();
	
	def receive = {
		case GetRequest(key) => {
			log.debug("GET {}", key);
			val resp = worker.get(key);
			log.debug("GET response: {}", resp);
			if (resp.code == ResponseCode.SUCCESS) {
				sender ! Entry(resp.key.toString(), new String(resp.data, Charset.forName("UTF-8")));
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
					case (k, v) => Entry(k.toString(), new String(v, Charset.forName("UTF-8")));
				}.toList);
			} else {
				sender ! Operation(resp.code);
			}
		}
	}
}