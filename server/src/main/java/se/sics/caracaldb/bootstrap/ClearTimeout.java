/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.bootstrap;

import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ClearTimeout extends Timeout {
    public ClearTimeout(SchedulePeriodicTimeout request) {
        super(request);
    }
}
