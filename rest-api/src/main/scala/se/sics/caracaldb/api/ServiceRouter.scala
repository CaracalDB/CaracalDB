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
import se.sics.datamodel.msg.DMMessage
import akka.routing._
import se.sics.caracaldb.api.data._
import java.nio.charset.Charset
import akka.event.LoggingAdapter
import se.sics.caracaldb.KeyRange
import se.sics.caracaldb.KeyRange.Bound

class ServiceRouterActor extends Actor with ServiceRouter with ActorLogging {

  def actorRefFactory = context

  def logger = log;

  def receive = runRoute(primaryRoute);

}

trait ServiceRouter extends HttpService with CORSDirectives {
  import CaracalJsonProtocol._
  import BasicDirectives._

  implicit val timeout = Timeout(30 seconds);

  def logger: LoggingAdapter;
  def corsExp = conf.getString("caracal.api.cors");

  val conf = Main.system.settings.config;
  val numWorkers = conf.getInt("caracal.api.workers");
  val numDMWorkers = conf.getInt("datamodel.workers");
  val workers = actorRefFactory.actorOf(Props[CaracalWorker].withRouter(SmallestMailboxRouter(numWorkers)), "caracal-workers");
  val dmWorkers = actorRefFactory.actorOf(Props[DataModelWorker].withRouter(SmallestMailboxRouter(numDMWorkers)), "datamodel-workers");

  import ExecutionDirectives._
  def debugHandler(implicit log: LoggingContext) = ExceptionHandler {
    case e => ctx =>
      log.warning("Request {} could not be handled normally: {}", ctx.request, e);
      e.printStackTrace();
      ctx.complete(InternalServerError, "An unknown error occurred. We apologize for this inconvenience.");
  }

  val detachAndRespond = respondWithMediaType(`application/json`) & handleExceptions(debugHandler) & detach();

  val primaryRoute: Route = {
    corsFilter(corsExp) {
      path("schema" / Segment) { schema =>
        get {
          detachAndRespond { ctx =>
            ctx.complete {
              rangeOp(schema, null, null);
            }
          }
        }
      } ~ pathPrefix("schema" / Segment) { schema =>
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
        } ~ path("range" / Segment / Segment) { (begin, end) =>
          get {
            detachAndRespond { ctx =>
              ctx.complete {
                rangeOp(schema, begin, end);
              }
            }
          }
        } ~ path("prefix" / Segment) { prefix =>
          get {
            detachAndRespond { ctx =>
              ctx.complete {
                rangeOp(schema, prefix, null);
              }
            }
          }
        }
      } ~ path("type") {
        entity(as[String]) { value =>
          post {
            detachAndRespond { ctx =>
              ctx.complete {
                objMGetPutOp(GetTypeRequest(value));
              }
            }
          } ~ put {
            detachAndRespond { ctx =>
              ctx.complete {
                objMGetPutOp(PutTypeRequest(value));
              }
            }
          }
        }
      } ~ path("object" / "q") {
        entity(as[String]) { value =>
          post {
            detachAndRespond { ctx =>
              ctx.complete {
                objMQuery(value);
              }
            }
          }
        }
      } ~ path("object") {
        entity(as[String]) { value =>
          post {
            detachAndRespond { ctx =>
              ctx.complete {
                objMGetPutOp(GetObjRequest(value));
              }
            }
          } ~ put {
            detachAndRespond { ctx =>
              ctx.complete {
                objMGetPutOp(PutObjRequest(value));
              }
            }
          }
        }
      }
    }
  }

  private def getOp(schemaStr: String, keyStr: String): Either[Entry, Operation] = {
    val key = KeyUtil.schemaToKey(schemaStr, keyStr);
    logger.debug("GET {}", key);
    val f = workers ? GetRequest(key);
    try {
      val res = Await.result(f, 10 seconds).asInstanceOf[CaracalResponse];
      logger.debug("GET result was {}", res);
      res match {
        case e: Entry => return Left(e);
        case o: Operation => return Right(o);
      }
    } catch {
      case e: TimeoutException => Right(Operation(ResponseCode.CLIENT_TIMEOUT));
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

  private def rangeOp(schemaStr: String, beginStr: String, endStr: String): Either[List[Entry], Operation] = {
    val begin = KeyUtil.schemaToKey(schemaStr, beginStr);
    val range = if (endStr == null) KeyRange.prefix(begin) else {
      val end = KeyUtil.schemaToKey(schemaStr, endStr);
      KeyRange.closed(begin).closed(end);
    }
    logger.debug("RQ {}", range);
    val f = workers ? RangeRequest(range);
    try {
      val res = Await.result(f, 10 seconds).asInstanceOf[CaracalResponse];
      logger.debug("GET result was {}", res);
      res match {
        case Entries(l) => return Left(l);
        case o: Operation => return Right(o);
      }
    } catch {
      case e: TimeoutException => Right(Operation(ResponseCode.CLIENT_TIMEOUT));
    }
  }

  private def objMGetPutOp(req: CaracalRequest): Either[FormattedResponse, DMOperation] = {
    val f = dmWorkers ? req;
    val res = Await.result(f, 10 seconds).asInstanceOf[CaracalResponse];
    logger.debug("ObjM OP result was {}", res);
    res match {
      case fr: FormattedResponse => return Left(fr);
      case o: DMOperation => return Right(o);
    }
  }

  private def objMQuery(str: String): Either[List[Entry], DMOperation] = {
    val f = dmWorkers ? QueryObjRequest(str);
    val res = Await.result(f, 10 seconds).asInstanceOf[CaracalResponse];
    logger.debug("ObjM OP result was {}", res);
    res match {
      case Entries(l) => return Left(l);
      case o: DMOperation => return Right(o);
    }
  }

}