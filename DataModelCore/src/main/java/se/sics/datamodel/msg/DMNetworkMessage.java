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
package se.sics.datamodel.msg;

import com.google.common.base.Objects;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Transport;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMNetworkMessage {

    public static class Req implements Msg {
        
        public final Address src;
        public final Address dst;
        public final DMMessage.Req payload;

        public Req(Address src, Address dst, DMMessage.Req payload) {
            this.src = src;
            this.dst = dst;
            this.payload = payload;
        }

        @Override
        public Address getSource() {
            return src;
        }

        @Override
        public Address getDestination() {
            return dst;
        }

        @Override
        public Address getOrigin() {
            return src;
        }

        @Override
        public Transport getProtocol() {
            return Transport.TCP;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 11 * hash + Objects.hashCode(this.src);
            hash = 11 * hash + Objects.hashCode(this.dst);
            hash = 11 * hash + Objects.hashCode(this.payload);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Req other = (Req) obj;
            if (!Objects.equal(this.src, other.src)) {
                return false;
            }
            if (!Objects.equal(this.dst, other.dst)) {
                return false;
            }
            if (!Objects.equal(this.payload, other.payload)) {
                return false;
            }
            return true;
        }
    }

    public static class Resp implements Msg {

        public final Address src;
        public final Address dst;
        public final DMMessage.Resp payload;

        public Resp(Address src, Address dst, DMMessage.Resp payload) {
            this.src = src;
            this.dst = dst;
            this.payload = payload;
        }

        @Override
        public Address getSource() {
            return src;
        }

        @Override
        public Address getDestination() {
            return dst;
        }

        @Override
        public Address getOrigin() {
            return src;
        }

        @Override
        public Transport getProtocol() {
            return Transport.TCP;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 47 * hash + Objects.hashCode(this.src);
            hash = 47 * hash + Objects.hashCode(this.dst);
            hash = 47 * hash + Objects.hashCode(this.payload);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Resp other = (Resp) obj;
            if (!Objects.equal(this.src, other.src)) {
                return false;
            }
            if (!Objects.equal(this.dst, other.dst)) {
                return false;
            }
            if (!Objects.equal(this.payload, other.payload)) {
                return false;
            }
            return true;
        }
    }
}