package se.sics.caracaldb.driver

import se.sics.caracaldb.driver.experiments._
import se.sics.kompics.network.Transport

object Experiments {
    val registered = Map(
        0 -> new SimpleTransfer(Transport.TCP),
        1 -> new SimpleTransfer(Transport.UDT),
        2 -> new SimpleTransfer(Transport.DATA),
        3 -> new SimplePings(Transport.TCP, 100),
        4 -> new SimplePings(Transport.UDT, 100),
        5 -> new PingTransfer(Transport.TCP, Transport.TCP),
        6 -> new PingTransfer(Transport.TCP, Transport.UDT),
        7 -> new PingTransfer(Transport.TCP, Transport.DATA),
        8 -> new TorrentTransfer()
    )
}