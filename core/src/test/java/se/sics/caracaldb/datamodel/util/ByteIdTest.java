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
package se.sics.caracaldb.datamodel.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
@RunWith(JUnit4.class)
public class ByteIdTest {

    @Test
    public void testFactory() {
        ByteIdFactory bif = new ByteIdFactory();
        Assert.assertEquals(new ByteId(new byte[]{1, 1}), bif.nextId());
        Assert.assertEquals(new ByteId(new byte[]{1, 2}), bif.nextId());
        for (int i = 0; i < 253; i++) {
            bif.nextId();
        }
        Assert.assertEquals(new ByteId(new byte[]{2, 1, 0}), bif.nextId());
        Assert.assertEquals(new ByteId(new byte[]{2, 1, 1}), bif.nextId());
        for (int i = 0; i < 253; i++) {
            bif.nextId();
        }
        for (int j = 1; j <= 255; j++) {
            for (int i = 0; i < 255; i++) {
                bif.nextId();
            }
        }
        Assert.assertEquals(new ByteId(new byte[]{3, 1, 0, 0}), bif.nextId());
    }
    
//    @Test
//    public void 
}
