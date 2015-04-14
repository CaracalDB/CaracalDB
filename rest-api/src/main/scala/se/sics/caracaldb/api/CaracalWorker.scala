package se.sics.caracaldb.api

import akka.actor._
import se.sics.caracaldb.client.ClientManager
import se.sics.caracaldb.Key
import se.sics.caracaldb.KeyRange
import se.sics.caracaldb.operations.ResponseCode
import se.sics.caracaldb.api.data._
import java.nio.charset.Charset

trait CaracalRequest
case class GetRequest(schema: String, key: Key) extends CaracalRequest
case class PutRequest(schema: String, key: Key, value: Array[Byte]) extends CaracalRequest
case class RangeRequest(schema: String, range: KeyRange) extends CaracalRequest
case class Schemas() extends CaracalRequest
case class LUT() extends CaracalRequest
case class Hosts() extends CaracalRequest

class CaracalWorker extends Actor with ActorLogging {
    import context._
    import scala.collection.JavaConversions._

    private val utf8 = Charset.forName("UTF-8");

    val worker = ClientManager.newClient();

    log.debug("Started CaracalWorker");

    def receive = {
        case GetRequest(schema, key) => {
            log.debug("GET {}", key);
            val resp = worker.get(schema, key);
            log.debug("GET response: {}", resp);
            if (resp.code == ResponseCode.SUCCESS) {
                val respStr = resp.data match {
                    case null           => "";
                    case x: Array[Byte] => new String(x, utf8);
                }
                sender ! Entry(resp.key.toString(), respStr);
            } else {
                sender ! Operation(resp.code);
            }
        }
        case PutRequest(schema, key, value) => {
            log.debug("PUT ({}, {})", key, value);
            val resp = worker.put(schema, key, value);
            log.debug("PUT response: {}", resp);
            sender ! Operation(resp);
        }
        case RangeRequest(schema, range) => {
            log.debug("Range {}", range);
            val resp = worker.rangeRequest(schema, range);
            log.debug("RangeRes: {}", resp);
            if (resp.code == ResponseCode.SUCCESS) {
                val res: scala.collection.mutable.Map[Key, Array[Byte]] = resp.results;
                val entries = Entries(res.flatMap {
                    case (k, v) => {
                        v match {
                            case null           => None;
                            case x: Array[Byte] => Some(Entry(k.toString(), new String(x, utf8)));
                        }
                    }
                }.toList);
                log.debug("Entries {}", entries);
                sender ! entries;
            } else {
                sender ! Operation(resp.code);
            }
        }
        case Schemas() => {
            sender ! FormattedResponse(worker.schemasAsJson());
        }
        case LUT() => {
            sender ! FormattedResponse(worker.lutAsJson());
        }
        case Hosts() => {
            sender ! FormattedResponse(worker.hostsAsJson());
        }
    }
}