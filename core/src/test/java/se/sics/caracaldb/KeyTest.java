/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
@RunWith(JUnit4.class)
public class KeyTest {
    @Test
    public void zeroTest() {
        Key nullKey = new Key(new byte[0]);
        Key zeroKey = new Key(new byte[] {0});
        assertTrue(nullKey.compareTo(zeroKey) < 0);
    }
}
