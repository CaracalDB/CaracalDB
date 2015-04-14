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
package se.sics.caracaldb.global;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.BaseMessage;
import se.sics.caracaldb.Header;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Transport;

/**
 *
 * @author lkroll
 */
public abstract class Schema {

    public static interface Req extends Msg<Address, Header>, LookupMessage {

        public Req forward(Address from, Address to);
    }

    public static class CreateReq extends BaseMessage implements Req {

        public final String name;
        public final ImmutableMap<String, String> metaData;

        public CreateReq(Address src, Address dst, String name, ImmutableMap<String, String> metaData) {
            super(src, dst, src, Transport.TCP);
            this.name = name;
            this.metaData = metaData;
        }

        CreateReq(Address src, Address dst, Address orig, String name, ImmutableMap<String, String> metaData) {
            super(src, dst, orig, Transport.TCP);
            this.name = name;
            this.metaData = metaData;
        }

        @Override
        public Req forward(Address from, Address to) {
            return new CreateReq(from, to, this.getOrigin(), name, metaData);
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof CreateReq) {
                return name.equals(((CreateReq) o).name);
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 37 * hash + (this.name != null ? this.name.hashCode() : 0);
            return hash;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("name", name)
                    .add("meta", metaData).toString();
        }

        public Response reply(Address src, byte[] schemaId) {
            return new Response(src, this.getOrigin(), name, schemaId, true, "Schema created.");
        }

    }

    public static class DropReq extends BaseMessage implements Req {

        public final String name;

        public DropReq(Address src, Address dst, String name) {
            super(src, dst, src, Transport.TCP);
            this.name = name;
        }

        DropReq(Address src, Address dst, Address orig, String name) {
            super(src, dst, orig, Transport.TCP);
            this.name = name;
        }

        @Override
        public Req forward(Address from, Address to) {
            return new DropReq(from, to, this.getOrigin(), name);
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof DropReq) {
                return name.equals(((DropReq) o).name);
            }
            return false;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 11 * hash + (this.name != null ? this.name.hashCode() : 0);
            return hash;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("name", name).toString();
        }

        public Response reply(Address src, byte[] schemaId) {
            return new Response(src, this.getOrigin(), name, schemaId, true, "Schema dropped.");
        }

    }

    public static class Response extends BaseMessage {

        public final String name;
        public final byte[] id;
        public final boolean success;
        public final String msg;

        public Response(Address src, Address dst, String name, byte[] id, boolean success, String msg) {
            super(src, dst, src, Transport.TCP);
            this.name = name;
            this.id = id;
            this.success = success;
            this.msg = msg;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("name", name)
                    .add("id", id)
                    .add("success?", success)
                    .add("msg", msg).toString();
        }

    }
}
