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
import se.sics.caracaldb.Key
import se.sics.caracaldb.KeyRange
import se.sics.caracaldb.KeyRange.Bound
import com.larskroll.common.CORSDirectives

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
    val workers = actorRefFactory.actorOf(Props[CaracalWorker].withRouter(SmallestMailboxPool(numWorkers)), "caracal-workers");

    import ExecutionDirectives._
    def debugHandler(implicit log: LoggingContext) = ExceptionHandler {
        case e => ctx =>
            log.warning("Request {} could not be handled normally: {}", ctx.request, e);
            e.printStackTrace();
            ctx.complete(InternalServerError, "An unknown error occurred. We apologize for this inconvenience.");
    }

    val detachAndRespond = respondWithMediaType(`application/json`) & handleExceptions(debugHandler) & detach(());

    val primaryRoute: Route = {
        corsFilter(corsExp) {
            options {
                complete("This is an OPTIONS request.")
            } ~ path("schemas") {
                get {
                    detachAndRespond { ctx =>
                        ctx.complete {
                            listSchemas();
                        }
                    }
                }
            } ~ path("hosts") {
                get {
                    detachAndRespond { ctx =>
                        ctx.complete {
                            listHosts();
                        }
                    }
                }
            } ~ path("system") {
                get {
                    detachAndRespond { ctx =>
                        ctx.complete {
                            getLUT();
                        }
                    }
                }
            } ~ path("schema" / Segment) { schema =>
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
            }
        }
    }

    private def getOp(schemaStr: String, keyStr: String): Either[Entry, Operation] = {
        val key = KeyUtil.stringToKey(keyStr);
        logger.debug("GET {}", key);
        val f = workers ? GetRequest(schemaStr, key);
        try {
            val res = Await.result(f, 10 seconds).asInstanceOf[CaracalResponse];
            logger.debug("GET result was {}", res);
            res match {
                case e: Entry     => return Left(e);
                case o: Operation => return Right(o);
            }
        } catch {
            case e: TimeoutException => Right(Operation(ResponseCode.CLIENT_TIMEOUT));
        }
    }

    private def putOp(schemaStr: String, keyStr: String, dataStr: String): Operation = {
        val key = KeyUtil.stringToKey(keyStr);
        logger.debug("PUT ({}, {})", key, dataStr);
        val f = workers ? PutRequest(schemaStr, key, dataStr match {
            case null      => null;
            case x: String => x.getBytes(Charset.forName("UTF-8"));
        });
        try {
            val res = Await.result(f, 10 seconds).asInstanceOf[Operation];
            logger.debug("PUT result was {}", res);
            return res;
        } catch {
            case e: TimeoutException => return Operation(ResponseCode.CLIENT_TIMEOUT);
        }
    }

    private def deleteOp(schemaStr: String, keyStr: String): Operation = {
        return putOp(schemaStr, keyStr, null); // this is not a true delete...but caracal doesn't really have delete support
    }

    private def rangeOp(schemaStr: String, beginStr: String, endStr: String): Either[List[Entry], Operation] = {
        val begin = KeyUtil.stringToKey(beginStr);
        val range = if (endStr == null) KeyRange.closed(begin).open(Key.INF) else {
            val end = KeyUtil.stringToKey(endStr);
            KeyRange.closed(begin).closed(end);
        }
        logger.debug("RQ {}", range);
        val f = workers ? RangeRequest(schemaStr, range);
        try {
            val res = Await.result(f, 10 seconds).asInstanceOf[CaracalResponse];
            logger.debug("GET result was {}", res);
            res match {
                case Entries(l)   => return Left(l);
                case o: Operation => return Right(o);
            }
        } catch {
            case e: TimeoutException => Right(Operation(ResponseCode.CLIENT_TIMEOUT));
        }
    }

    private def listSchemas(): FormattedResponse = {
        val f = workers ? Schemas();
        val res = Await.result(f, 10 seconds).asInstanceOf[FormattedResponse];
        return res;
    }

    private def getLUT(): FormattedResponse = {
        val f = workers ? LUT();
        val res = Await.result(f, 10 seconds).asInstanceOf[FormattedResponse];
        return res;
    }

    private def listHosts(): FormattedResponse = {
        val f = workers ? Hosts();
        val res = Await.result(f, 10 seconds).asInstanceOf[FormattedResponse];
        return res;
    }

}