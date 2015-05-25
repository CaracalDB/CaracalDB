package se.sics.caracaldb.driver

import se.sics.caracaldb.driver.experiments._
import se.sics.kompics.network.Transport

object Experiments {
    val registered = Map(
        0 -> new SimpleTransfer(Transport.TCP),
        1 -> new SimpleTransfer(Transport.UDT),
        2 -> new SimplePings(Transport.TCP, 100),
        3 -> new SimplePings(Transport.UDT, 100),
        4 -> new PingTransfer(Transport.TCP, Transport.TCP),
        5 -> new PingTransfer(Transport.UDT, Transport.TCP),
        6 -> new PingTransfer(Transport.UDT, Transport.UDT)
    )
}