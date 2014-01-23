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
package se.sics.caracaldb.simulation.operations.datamodel;

import java.util.LinkedList;
import java.util.List;
import org.javatuples.Pair;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.simulation.operations.datamodel.validators.RespValidator;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMExperiment {

    private final List<Pair<DMMessage.Req, RespValidator>> msgs;
    private int currentMsg;
    
    private DMExperiment(List<Pair<DMMessage.Req, RespValidator>> msgs) {
        this.msgs = msgs;
        this.currentMsg = -1;
    }

    public boolean isDone() {
        return msgs.size() == currentMsg + 1;
    }
    
    public DMMessage.Req nextReq() {
        currentMsg++;
        return msgs.get(currentMsg).getValue0();
    }

    public void validate(DMMessage.Resp resp) {
        msgs.get(currentMsg).getValue1().validate(resp);
    }
    
    public static class Builder {
        private final List<Pair<DMMessage.Req, RespValidator>> msgs;
        
        public Builder() {
            msgs = new LinkedList<Pair<DMMessage.Req, RespValidator>>();
        }
        
        public void add(DMMessage.Req req, RespValidator validator) {
            msgs.add(Pair.with(req, validator));
        }
        
        public DMExperiment build() {
            return new DMExperiment(msgs);
        }
    }
}
