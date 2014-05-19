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
package se.sics.datamodel.simulation.validators;

import java.util.Arrays;
import java.util.UUID;
import org.junit.Assert;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.msg.GetType;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

public class GetTypeValidator implements RespValidator {
    private final UUID id;
    private final DMMessage.ResponseCode respCode;
    private final byte[] typeInfo;

    public GetTypeValidator(UUID id, DMMessage.ResponseCode respCode, byte[] typeInfo) {
        this.id = id;
        this.respCode = respCode;
        this.typeInfo = typeInfo;
    }
    
    @Override
    public void validate(DMMessage.Resp resp) {
        Assert.assertEquals("Wrong message", id, resp.id);
        if (!(resp instanceof GetType.Resp)) {
            Assert.assertTrue(false);
        }
        Assert.assertEquals("Wrong ResponseCode", respCode, resp.respCode);
        
        GetType.Resp typedResp = (GetType.Resp) resp;
        Assert.assertTrue("Type value is not the same", Arrays.equals(typeInfo, typedResp.typeInfo));
    }
 
    @Override
    public String toString() {
        return "GET_TYPE - validator(" + id + ")";
    }
}