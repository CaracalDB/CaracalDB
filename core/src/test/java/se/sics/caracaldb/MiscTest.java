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
package se.sics.caracaldb;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 * @author sario
 */
@RunWith(JUnit4.class)
public class MiscTest {

    @Test
    public void thisTest() {
        Subclass subby = new Subclass();
        Superclass suppy = new Subclass();
        String subName = Subclass.class.getCanonicalName();
        assertEquals(subName, subby.myClass());
        assertEquals(subName, suppy.myClass());
    }

    public abstract static class Superclass {

        public String myClass() {
            return this.getClass().getCanonicalName();
        }
    }

    public static class Subclass extends Superclass {

    }
}
