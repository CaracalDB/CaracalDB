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
import se.sics.caracaldb.api.data.Entry
import se.sics.caracaldb.api.data.CaracalJsonProtocol._
import se.sics.caracaldb.api.data.Operation
import se.sics.caracaldb.operations.ResponseCode

class ServiceRouterActor extends Actor with ServiceRouter with ActorLogging {

	def actorRefFactory = context

	def receive = runRoute(primaryRoute);

}

trait ServiceRouter extends HttpService {
	import BasicDirectives._

	implicit val timeout = Timeout(30 seconds);

	val conf = Main.system.settings.config;

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

	private def getOp(schemaStr: String, keyStr: String): Entry = {
		return Entry(schemaStr, keyStr);
	}

	private def putOp(schemaStr: String, keyStr: String, dataStr: String): Operation = {
		return Operation(ResponseCode.SUCCESS);
	}
	
	private def deleteOp(schemaStr: String, keyStr: String): Operation = {
		return Operation(ResponseCode.SUCCESS);
	}

}