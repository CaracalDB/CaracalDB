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

import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
@RunWith(JUnit4.class)
public class KeyTest {

//    @Test
//    public void zeroTest() {
//        assertTrue(Key.NULL_KEY.compareTo(Key.ZERO_KEY) < 0);
//    }
//
//    @Test
//    public void keyBuilderTest() {
//        Key k = new Key(new byte[]{1, 2, 3, 4, 5, 6});
//        System.out.println("k: " + k.toString());
//        Key oneKey = new Key((byte) 1);
//        Key sixKey = new Key((byte) 6);
//        Key twoToFiveKey = new Key(new byte[]{2, 3, 4, 5});
//        Key k2 = oneKey.append(twoToFiveKey).append(sixKey).get();
//        System.out.println("k2: " + k2.toString());
//        assertTrue(k.compareTo(k2) == 0);
//        Key k3 = sixKey.prepend(twoToFiveKey).prepend(oneKey).get();
//        System.out.println("k3: " + k3.toString());
//        assertTrue(k.compareTo(k3) == 0);
//        Key k4 = twoToFiveKey.append(sixKey).prepend(oneKey).get();
//        System.out.println("k4: " + k4.toString());
//        assertTrue(k.compareTo(k4) == 0);
//    }
//
//    @Test
//    public void incTest() {
//        assertTrue(Key.NULL_KEY.inc().compareTo(Key.ZERO_KEY) == 0);
//        Key fKey = new Key(-1);
//        System.out.println("F-Key: " + fKey.toString());
//        System.out.println("F-Key INC: " + fKey.inc().toString());
//        assertTrue(fKey.inc().compareTo(Key.INF) == 0);
//        assertTrue(Key.ZERO_KEY.inc().compareTo(new Key((byte) 1)) == 0);
//    }

    @Test
    public void zeroTest() {
        assertTrue(Key.NULL_KEY.compareTo(Key.ZERO_KEY) < 0);
    }
    
    @Test
    public void keyBuilderTest() {
        Key k = new Key(new byte[] {1, 2, 3, 4, 5, 6});
        System.out.println("k: " + k.toString());
        Key oneKey = new Key((byte)1);
        Key sixKey = new Key((byte)6);
        Key twoToFiveKey = new Key(new byte[] {2, 3, 4, 5});
        Key k2 = oneKey.append(twoToFiveKey).append(sixKey).get();
        System.out.println("k2: " + k2.toString());
        assertTrue(k.compareTo(k2) == 0);
        Key k3 = sixKey.prepend(twoToFiveKey).prepend(oneKey).get();
        System.out.println("k3: " + k3.toString());
        assertTrue(k.compareTo(k3) == 0);
        Key k4 = twoToFiveKey.append(sixKey).prepend(oneKey).get();
        System.out.println("k4: " + k4.toString());
        assertTrue(k.compareTo(k4) == 0);        
    }
    
    @Test
    public void incTest() {
        assertTrue(Key.NULL_KEY.inc().compareTo(Key.ZERO_KEY) == 0);
        Key fKey = new Key(-1);
        System.out.println("F-Key: " + fKey.toString());
        System.out.println("F-Key INC: " + fKey.inc().toString());
        assertTrue(fKey.inc().compareTo(Key.INF) == 0);
        assertTrue(Key.ZERO_KEY.inc().compareTo(new Key((byte)1)) == 0);
        Key someSigKey = Key.fromHex("09 8F 6B CD 46 21 D3 73 CA DE 4E 83 26 27 B4 06");
        Key someSigKeyInc = Key.fromHex("09 8F 6B CD 46 21 D3 73 CA DE 4E 83 26 27 B4 07");
        System.out.println("Signed Key");
        assertTrue(someSigKey.inc().compareTo(someSigKeyInc) == 0);
        Key someKey = Key.fromHex("09 8F 6B CD 46 21 D3 73 CA DE 4E 83 26 27 B4 F6");
        Key someKeyInc = Key.fromHex("09 8F 6B CD 46 21 D3 73 CA DE 4E 83 26 27 B4 F7");
        System.out.println("Not so signed Key");
        assertTrue(someKey.inc().compareTo(someKeyInc) == 0);
    }
}
