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
package se.sics.caracaldb.simulation.operations.datamodel.validators;

import org.junit.Assert;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.datamodel.msg.PutType;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class PutTypeValidator implements RespValidator {
    private final long id;
    private final DMMessage.ResponseCode respCode;

    public PutTypeValidator(long id, DMMessage.ResponseCode respCode) {
        this.id = id;
        this.respCode = respCode;
    }
    
    @Override
    public void validate(DMMessage.Resp resp) {
        Assert.assertEquals("Wrong message", id, resp.id);
        if (!(resp instanceof PutType.Resp)) {
            Assert.assertTrue(false);
        }

        Assert.assertEquals("Wrong ResponseCode", respCode, resp.respCode);
    }
    
    @Override
    public String toString() {
        return "PUT_TYPE - validator(" + id + ")";
    }
}
