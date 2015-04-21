package se.sics.caracaldb.api

import akka.actor._
import akka.pattern.pipe
import se.sics.caracaldb.client.ClientManager
import se.sics.caracaldb.Key
import se.sics.caracaldb.KeyRange
import se.sics.caracaldb.operations.ResponseCode
import se.sics.caracaldb.global.Schema
import se.sics.caracaldb.global.LUTJsonProtocol.SchemaObj
import se.sics.caracaldb.api.data._
import com.larskroll.common.ByteArrayFormatter
import java.nio.charset.Charset
import scala.concurrent.Future

trait CaracalRequest
case class GetRequest(schema: String, key: Key) extends CaracalRequest
case class PutRequest(schema: String, key: Key, value: Array[Byte]) extends CaracalRequest
case class RangeRequest(schema: String, range: KeyRange) extends CaracalRequest
case class Schemas() extends CaracalRequest
case class CreateSchema(schema: SchemaObj) extends CaracalRequest
case class DropSchema(schema: String) extends CaracalRequest
case class LUT() extends CaracalRequest
case class Hosts() extends CaracalRequest

class CaracalWorker extends Actor with ActorLogging {
    import context._
    import scala.collection.JavaConversions._
    import com.larskroll.common.FutureConversions._

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
        case CreateSchema(schema) => {
            val resp: Future[Schema.Response] = worker.createSchema(schema.getName(), schema.getMeta());
            resp.map { sr =>
                val id = if (sr.id != null) {
                    Some(ByteArrayFormatter.printFormat(sr.id));
                } else {
                    None
                }
                val msg = if (sr.msg != null) {
                    Some(sr.msg)
                } else {
                    None
                }
                SchemaResponse(sr.name, id, sr.success, msg)
            } pipeTo sender;
        }
        case DropSchema(schema) => {
            val resp: Future[Schema.Response] = worker.dropSchema(schema);
            resp.map { sr =>
                val id = if (sr.id != null) {
                    Some(ByteArrayFormatter.printFormat(sr.id));
                } else {
                    None
                }
                val msg = if (sr.msg != null) {
                    Some(sr.msg)
                } else {
                    None
                }
                SchemaResponse(sr.name, id, sr.success, msg)
            } pipeTo sender;
        }
        case LUT() => {
            sender ! FormattedResponse(worker.lutAsJson());
        }
        case Hosts() => {
            sender ! FormattedResponse(worker.hostsAsJson());
        }
    }
}