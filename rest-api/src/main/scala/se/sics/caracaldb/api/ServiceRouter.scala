package se.sics.caracaldb.api

import spray.util._
import spray.http._
import akka.actor.{ ActorLogging, Actor }
import akka.pattern.{ ask, pipe }
import scala.concurrent._
import scala.concurrent.duration._
import akka.util.Timeout
import spray.routing.{ HttpService, Route }
import spray.routing.directives._
import spray.httpx.encoding._
import spray.http._
import spray.http.MediaTypes._
import spray.http.HttpCharsets._
import spray.http.StatusCodes._
import spray.json._
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.routing.ExceptionHandler
import akka.actor.Props
import se.sics.caracaldb.operations.ResponseCode
import akka.routing._
import se.sics.caracaldb.api.data._
import java.nio.charset.Charset
import akka.event.LoggingAdapter

class ServiceRouterActor extends Actor with ServiceRouter with ActorLogging {

	def actorRefFactory = context
	
	def logger = log;

	def receive = runRoute(primaryRoute);

}

trait ServiceRouter extends HttpService {
	import CaracalJsonProtocol._
	import BasicDirectives._

	implicit val timeout = Timeout(30 seconds);
	
	def logger: LoggingAdapter;

	val conf = Main.system.settings.config;
	val numWorkers = conf.getInt("caracal.api.workers");
	val workers = actorRefFactory.actorOf(Props[CaracalWorker].withRouter(SmallestMailboxRouter(numWorkers)), "caracal-workers");

	import ExecutionDirectives._
	def debugHandler(implicit log: LoggingContext) = ExceptionHandler {
		case e => ctx =>
			log.warning("Request {} could not be handled normally", ctx.request);
			ctx.complete(InternalServerError, "An unknown error occurred. We apologize for this inconvenience.");
	}

	val detachAndRespond = respondWithMediaType(`application/json`) & handleExceptions(debugHandler) & detach();

	val primaryRoute: Route = {
		pathPrefix("schema" / Segment) { schema =>
			path("key" / Segment) { key =>
				get {
					detachAndRespond { ctx =>
						ctx.complete {
							getOp(schema, key);
						}
					}
				} ~ (post | put) {
					entity(as[String]) { value =>
						detachAndRespond { ctx =>
							ctx.complete {
								putOp(schema, key, value);
							}
						}
					}
				} ~ delete {
					detachAndRespond { ctx =>
						ctx.complete {
							deleteOp(schema, key);
						}
					}
				}
			}
		}

	}

	private def getOp(schemaStr: String, keyStr: String): Option[Entry] = {
		val key = KeyUtil.schemaToKey(schemaStr, keyStr);
		logger.debug("GET {}", key);
		val f = workers ? GetRequest(key);
		try {
			val res = Await.result(f, 10 seconds).asInstanceOf[CaracalResponse];
			logger.debug("GET result was {}", res);
			res match {
				case e: Entry => return Some(e);
				case o: Operation => return None;
			}
		} catch {
			case e: TimeoutException => None;
		}
	}

	private def putOp(schemaStr: String, keyStr: String, dataStr: String): Operation = {
		val key = KeyUtil.schemaToKey(schemaStr, keyStr);
		logger.debug("PUT ({}, {})", key, dataStr);
		val f = workers ? PutRequest(key, dataStr.getBytes(Charset.forName("UTF-8")));
		try {
			val res = Await.result(f, 10 seconds).asInstanceOf[Operation];
			logger.debug("PUT result was {}", res);
			return res;
		} catch {
			case e: TimeoutException => return Operation(ResponseCode.CLIENT_TIMEOUT);
		}
	}

	private def deleteOp(schemaStr: String, keyStr: String): Operation = {
		return putOp(schemaStr, keyStr, ""); // this is not a true delete...but caracal doesn't really have delete support
	}

}