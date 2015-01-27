package se.sics.caracaldb.api
 
import spray.routing._
import spray.http._
import spray.http.StatusCodes.Forbidden
 
 
// See https://developer.mozilla.org/en-US/docs/HTTP/Access_control_CORS
 
case class Origin(origin: String) extends HttpHeader {
  def name = "Origin"
  def lowercaseName = "origin"
  def value = origin
  def render[R <: Rendering](r: R): r.type = r ~~ name ~~ ':' ~~ ' ' ~~ value
}
 
case class `Access-Control-Allow-Origin`(origin: String) extends HttpHeader {
  def name = "Access-Control-Allow-Origin"
  def lowercaseName = "access-control-allow-origin"
  def value = origin
  def render[R <: Rendering](r: R): r.type = r ~~ name ~~ ':' ~~ ' ' ~~ value
}

case class `Access-Control-Allow-Methods`(methods: String) extends HttpHeader {
  def name = "Access-Control-Allow-Methods"
  def lowercaseName = "access-control-allow-methods"
  def value = methods
  def render[R <: Rendering](r: R): r.type = r ~~ name ~~ ':' ~~ ' ' ~~ value
}
 
case class `Access-Control-Allow-Credentials`(allowed: Boolean) extends HttpHeader {
  def name = "Access-Control-Allow-Credentials"
  def lowercaseName = "access-control-allow-credentials"
  def value = if(allowed) "true" else "false"
  def render[R <: Rendering](r: R): r.type = r ~~ name ~~ ':' ~~ ' ' ~~ value
}
 
trait CORSDirectives { this: HttpService =>
  def respondWithCORSHeaders(origin: String) =
    respondWithHeaders(
      `Access-Control-Allow-Origin`(origin),
      `Access-Control-Allow-Credentials`(true),
      `Access-Control-Allow-Methods`("GET, PUT, POST, DELETE"))
 
  def corsFilter(origin: String)(route: Route) =
    if(origin == "*")
      respondWithCORSHeaders("*")(route)
    else
      optionalHeaderValueByName("Origin") {
        case None => route
        case Some(clientOrigin) =>
          if(origin == clientOrigin)
            respondWithCORSHeaders(origin)(route)
          else
            complete(Forbidden, Nil, "Invalid origin")  // Maybe, a Rejection will fit better
      }
}