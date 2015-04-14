/* 
 * This file is part of the CaracalDB distributed storage system.
 *
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) 
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.caracaldb.system;

import se.sics.caracaldb.Address;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Init;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.network.virtual.VirtualNetworkChannel;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class LauncherComponent extends ComponentDefinition {

    private Component network;
    private Component deads;
    private Component timer;
    private Component manager;
    VirtualNetworkChannel vnc;

    

    {
        Configuration config = Launcher.getConfig();
        Address netSelf = new Address(config.getIp(), config.getInt("server.address.port"), null);
        TimestampIdFactory.init(netSelf);
        network = create(NettyNetwork.class, new NettyInit(netSelf));
        timer = create(JavaTimer.class, Init.NONE);
        deads = create(DeadLetterBox.class, new DeadLetterBoxInit(netSelf));
        vnc = VirtualNetworkChannel.connect(network.getPositive(Network.class), deads.getNegative(Network.class));
        manager = create(HostManager.class, new HostManagerInit(config, netSelf, vnc));

        connect(manager.getNegative(Timer.class), timer.getPositive(Timer.class));

    }

}
