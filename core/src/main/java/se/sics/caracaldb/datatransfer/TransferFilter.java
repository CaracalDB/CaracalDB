/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.datatransfer;

import se.sics.kompics.ChannelFilter;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class TransferFilter extends ChannelFilter<TransferMessage, Long> {

    public TransferFilter(Long val) {
        super(TransferMessage.class, val, true);
    }

    @Override
    public Long getValue(TransferMessage event) {
        return event.id;
    }
}
