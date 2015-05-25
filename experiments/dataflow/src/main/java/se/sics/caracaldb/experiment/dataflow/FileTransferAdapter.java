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

import com.google.common.hash.HashCode;
import com.larskroll.common.PartialFileRef;
import com.larskroll.common.RAFileRef;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.datatransfer.Data;
import se.sics.caracaldb.datatransfer.DataSink;
import se.sics.caracaldb.datatransfer.DataSource;
import se.sics.caracaldb.flow.FileChunkCollector;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Start;

/**
 *
 * @author lkroll
 */
public class FileTransferAdapter extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(FileTransferAdapter.class);

    // Ports
    Negative<DataSource> source = provides(DataSource.class);
    Negative<DataSink> sink = provides(DataSink.class);
    // Instance
    private final File file;
    private final Mode mode;
    private final HashCode hash;
    private RAFileRef raf;

    public FileTransferAdapter(Init init) {
        this.file = init.file;
        this.mode = init.mode;
        this.hash = init.hash;

        subscribe(startHandler, control);
        switch (mode) {
            case SOURCE:
                subscribe(requestHandler, source);
            case SINK:
                subscribe(dataHandler, sink);
        }
    }

    // Handlers
    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            if (mode == Mode.SOURCE) {
                try {
                    RandomAccessFile ra = new RandomAccessFile(file, "r");
                    raf = new RAFileRef(file, ra);
                    trigger(new Data.Requirements(raf.size()), source);
                } catch (FileNotFoundException ex) {
                    LOG.error("File does not exist!", ex);
                    throw new RuntimeException(ex); // let supervisor handle cleanup
                }
            }
        }
    };
    Handler<Data.Request> requestHandler = new Handler<Data.Request>() {

        @Override
        public void handle(Data.Request event) {
            if (event.size >= raf.size()) {
                trigger(new Data.Reference(raf, FileChunkCollector.descriptor(raf.size()), true), source);
            } else {
                //FIXME split into parts
                LOG.error("Didn't get enough space allocation for file...ignoring. Required: {} - Got: {}", raf.size(), event.size);
            }
        }
    };
    Handler<Data.Reference> dataHandler = new Handler<Data.Reference>() {

        @Override
        public void handle(Data.Reference event) {
            if (event.isFinal) {
                try {
                    File target = file;
                    while (target.exists()) {
                        target = new File(target.getCanonicalPath() + ".moved");
                        LOG.info("File {} exists. Moving data to new file: {}", file.getCanonicalPath());
                    }
                    //RandomAccessFile raf = new RandomAccessFile(target, "rws");
                    PartialFileRef pfr = (PartialFileRef) event.data; // this better work^^
                    RAFileRef rafr = pfr.fileRef();
                    Path src = rafr.getFile().toPath();
                    Path tgt = target.toPath();
                    //RAFileRef rafr = new RAFileRef(target, raf);
                    LOG.info("Copying tmp data to final location {}...", target.getCanonicalPath());
                    //pfr.copyTo(rafr, 0);
                    pfr.release();
                    rafr.release();
                    //LOG.error("RAFR still has RC = " + rafr.rc());
                    Files.move(src, tgt, REPLACE_EXISTING);                    
                    LOG.info("Finished copying tmp data to final location {}...", target.getCanonicalPath());
                    HashCode newHash = Main.getHash(target);
                    if (hash.equals(newHash)) {
                        LOG.info("Hashcode matches, file transferred successfully!");
                    } else {
                        LOG.warn("Hashcodes do not match, file has been corrupted during transfer!");
                    }
                    trigger(Data.AllWritten.event, sink);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                //FIXME reassemble parts
                LOG.error("Got a part of a file, but don't know how to deal with it, yet");
            }
        }
    };

    public static enum Mode {

        SOURCE,
        SINK;
    }

    public static class Init extends se.sics.kompics.Init<FileTransferAdapter> {

        public final File file;
        public final Mode mode;
        public final HashCode hash;

        public Init(File file, Mode mode, HashCode hash) {
            this.file = file;
            this.mode = mode;
            this.hash = hash;
        }
    }
}
