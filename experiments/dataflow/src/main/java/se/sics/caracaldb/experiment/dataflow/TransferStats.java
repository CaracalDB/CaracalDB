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
package se.sics.caracaldb.experiment.dataflow;

import java.io.Serializable;

/**
 *
 * @author lkroll
 */
public class TransferStats implements Serializable{
    public final double averageThroughput;
    public final long time;
    public final long bytes;
    
    public TransferStats(long bytes, long time) {
        this.bytes = bytes;
        this.time = time;
        this.averageThroughput = ((double)this.bytes)/((double)this.time);
    }
    
    @Override
    public String toString() {
        double diffs = (double) time / 1000.0;
                    double fsizekb = (double) bytes / 1024.0;
                    double avg = fsizekb / diffs; // in kb/s
        
        StringBuilder sb = new StringBuilder();
        sb.append("TransferStats{\n");
        sb.append("   time: ");
        sb.append(diffs);
        sb.append("s\n");
        sb.append("   filesize: ");
        sb.append(fsizekb);
        sb.append("kb\n");
        sb.append("   avg. throughput: ");
        sb.append(avg);
        sb.append("kb/s");
        sb.append("}");
        return sb.toString();
    }
}
