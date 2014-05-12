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

import java.util.Map;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.msg.GetAllTypes;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.simulation.DMOp1Component;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GetAllTypesValidator implements RespValidator {

    private static final Logger LOG = LoggerFactory.getLogger(DMOp1Component.class);

    private final long id;
    private final DMMessage.ResponseCode respCode;
    private final ByteId dbId;
    private final Map<String, ByteId> types;

    public GetAllTypesValidator(long id, DMMessage.ResponseCode respCode, ByteId dbId, Map<String, ByteId> types) {
        this.id = id;
        this.respCode = respCode;
        this.dbId = dbId;
        this.types = types;
    }

    @Override
    public void validate(DMMessage.Resp resp) {
        Assert.assertEquals("Wrong message", id, resp.id);
        if (!(resp instanceof GetAllTypes.Resp)) {
            Assert.assertTrue(false);
        }
        GetAllTypes.Resp typedResp = (GetAllTypes.Resp) resp;

        Assert.assertEquals("Wrong ResponseCode", respCode, typedResp.respCode);
        if (respCode.equals(DMMessage.ResponseCode.FAILURE)) {
            return;
        } else {
            Assert.assertEquals(dbId, typedResp.dbId);
            Assert.assertNotNull(typedResp.types);
            Assert.assertEquals(types.size(), typedResp.types.size());
            for (Map.Entry<String, ByteId> type : typedResp.types.entrySet()) {
                ByteId expectedTypeId = types.get(type.getKey());
                Assert.assertNotNull(expectedTypeId);
                Assert.assertEquals(expectedTypeId, type.getValue());
            }
        }
    }

    @Override
    public String toString() {
        return "GET_ALLTYPES - validator(" + id + ")";
    }
}
