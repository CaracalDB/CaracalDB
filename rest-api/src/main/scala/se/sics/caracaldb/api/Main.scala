package se.sics.caracaldb.api

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http

object Main extends App with MySslConfiguration {

	// we need an ActorSystem to host our application in
	implicit val system = ActorSystem("whloot");
	
	// get host settings from config
	val hostname = system.settings.config.getString("caracal.api.host.hostname");
	val port = system.settings.config.getInt("caracal.api.host.port");
	
	// the handler actor replies to incoming HttpRequests
	val handler = system.actorOf(Props[ServiceRouterActor], "service-router");

	
	// create a new HttpServer using our handler tell it where to bind to
	IO(Http) ! Http.Bind(handler, interface = hostname, port = port);
}