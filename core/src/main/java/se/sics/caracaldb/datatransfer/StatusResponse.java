/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.datatransfer;

import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class StatusResponse extends Response {
    
    public final long dataSent;
    public final long itemsSent;
    public final DataSender.State currentState;
    public final long id;
    
    public StatusResponse(StatusRequest req, long id, long dataSent, 
            long itemsSent, DataSender.State currentState) {
        super(req);
        this.id = id;
        this.dataSent = dataSent;
        this.itemsSent = itemsSent;
        this.currentState = currentState;
    }
    
}
