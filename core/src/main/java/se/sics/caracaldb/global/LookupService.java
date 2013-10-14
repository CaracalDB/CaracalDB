/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.kompics.PortType;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class LookupService extends PortType {

    {
        request(ForwardToAny.class);
        request(ForwardToRange.class);
        request(LookupRequest.class);
        indication(LookupResponse.class);
    }
}
