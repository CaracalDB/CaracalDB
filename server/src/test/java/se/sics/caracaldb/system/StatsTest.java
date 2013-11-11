/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.sics.kompics.address.Address;
import static org.junit.Assert.*;
import se.sics.caracaldb.system.Stats.Report;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
@RunWith(JUnit4.class)
public class StatsTest {
    
    private static final int NUM = 100;
    private static final int TIME = 100;
    
    private Address self = null;
    
    @Before
    public void setUp() {
        InetAddress localHost = null;
        try {
            localHost = InetAddress.getByName("127.0.0.1");
        } catch (UnknownHostException ex) {
            fail(ex.getMessage());
        }
        int port = 22333;

        self = new Address(localHost, port, null);
    }

    @Test
    public void basic() {
        
        for (int i = 0; i < NUM; i++) {
            Report r = Stats.collect(self);
            assertNotNull(r);
            System.out.println(r);
            try {
                Thread.sleep(TIME);
            } catch (InterruptedException ex) {
                Logger.getLogger(StatsTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }
}
