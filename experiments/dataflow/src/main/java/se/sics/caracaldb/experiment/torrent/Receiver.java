/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.experiment.torrent;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.experiment.dataflow.MessageRegistrator;
import se.sics.caracaldb.experiment.dataflow.TransferStats;
import se.sics.gvod.core.util.TorrentDetails;
import se.sics.gvod.network.GVoDSerializerSetup;
import se.sics.gvod.stream.StreamHostComp;
import se.sics.gvod.stream.report.ReportPort;
import se.sics.gvod.stream.report.SummaryEvent;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.managedStore.core.FileMngr;
import se.sics.ktoolbox.util.managedStore.core.HashMngr;
import se.sics.ktoolbox.util.managedStore.core.impl.StorageMngrFactory;
import se.sics.ktoolbox.util.managedStore.core.impl.TransferMngr;
import se.sics.ktoolbox.util.managedStore.core.util.HashUtil;
import se.sics.ktoolbox.util.managedStore.core.util.Torrent;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.basic.BasicAddress;
import se.sics.ktoolbox.util.network.nat.NatAwareAddressImpl;
import se.sics.ktoolbox.util.setup.BasicSerializerSetup;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class Receiver extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(Receiver.class);
    private String logPrefix = "";

    private Component networkComp;
    private Component timerComp;
    private Component streamComp;

    Positive<ReportPort> reportPort = requires(ReportPort.class);

    private final ExperimentKCWrapper experimentConfig;

    private final KAddress selfAdr;
    private KAddress senderAdr = null;
    private final File outputDir;
    public SettableFuture<TransferStats> promise;

    public Receiver(Init init) {
        experimentConfig = new ExperimentKCWrapper(config());
        selfAdr = NatAwareAddressImpl.open(new BasicAddress(init.selfAdr.getIp(), init.selfAdr.getPort(), experimentConfig.receiverId));
        LOG.debug("{}starting...", logPrefix);

        if (init.senderAdr != null) {
            senderAdr = NatAwareAddressImpl.open(new BasicAddress(init.senderAdr.getIp(), init.senderAdr.getPort(), experimentConfig.senderId));
        }
        outputDir = init.outputDir;

        registerSerializers();
        registerPortTracking();

        subscribe(handleStart, control);
        subscribe(handleStartTransfer, loopback);
        subscribe(handleSummary, reportPort);
    }

    private void registerSerializers() {
        MessageRegistrator.register();
        int currentId = 128;
        currentId = BasicSerializerSetup.registerBasicSerializers(currentId);
        currentId = GVoDSerializerSetup.registerSerializers(currentId);
    }

    private void registerPortTracking() {
    }

    public ListenableFuture<TransferStats> startTransfer(Address dst) {
        StartTransferEvent st = new StartTransferEvent(dst);
        trigger(st, onSelf);
        return st.promise;
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);
            timerComp = create(JavaTimer.class, Init.NONE);
            networkComp = create(NettyNetwork.class, new NettyInit(selfAdr));
            trigger(Start.event, timerComp.control());
            trigger(Start.event, networkComp.control());

            if (senderAdr != null) {
                connect();
                trigger(Start.event, streamComp.control());
            }
        }
    };

    Handler handleStartTransfer = new Handler<StartTransferEvent>() {
        @Override
        public void handle(StartTransferEvent event) {
            Receiver.this.senderAdr = NatAwareAddressImpl.open(new BasicAddress(event.senderAdr.getIp(), event.senderAdr.getPort(), experimentConfig.senderId));
            Receiver.this.promise = event.promise;
            connect();
            trigger(Start.event, streamComp.control());
        }
    };

    Handler handleSummary = new Handler<SummaryEvent>() {
        @Override
        public void handle(SummaryEvent event) {
            TransferStats stats = new TransferStats(event.transferSize, event.transferTime);
            LOG.info("{}transfer of:{} completed in:{}", new Object[]{logPrefix, event.transferSize, event.transferTime});
            if (promise != null) {
                disconnect();
                trigger(Kill.event, streamComp.control());
                promise.set(stats);
            }
        }
    };

    private void disconnect() {
        disconnect(streamComp.getPositive(ReportPort.class), reportPort.getPair());
    }

    private void connect() {

        StreamHostComp.ExtPort extPorts = new StreamHostComp.ExtPort(timerComp.getPositive(Timer.class), networkComp.getPositive(Network.class));
        TorrentDetails torrentDetails = new TorrentDetails() {
            private final Identifier overlayId = experimentConfig.torrentId;
            private final String filePath = outputDir + File.separator + experimentConfig.torrentName;
            private final String hashPath = outputDir + File.separator + experimentConfig.torrentName + ".hash";

            @Override
            public Identifier getOverlayId() {
                return overlayId;
            }

            @Override
            public boolean download() {
                return true;
            }

            @Override
            public Torrent getTorrent() {
                throw new RuntimeException("logic error");
            }

            @Override
            public Triplet<FileMngr, HashMngr, TransferMngr> torrentMngrs(Torrent torrent) {
                try {
                    int blockSize = torrent.torrentInfo.piecesPerBlock * torrent.torrentInfo.pieceSize;
                    int hashSize = HashUtil.getHashSize(torrent.torrentInfo.hashAlg);

                    FileMngr fileMngr = StorageMngrFactory.incompleteMMFileMngr(filePath, torrent.fileInfo.size, blockSize, torrent.torrentInfo.pieceSize);
                    HashMngr hashMngr = StorageMngrFactory.incompleteMMHashMngr(hashPath, torrent.torrentInfo.hashAlg, torrent.torrentInfo.hashFileSize, hashSize);
                    return Triplet.with(fileMngr, hashMngr, new TransferMngr(torrent, hashMngr, fileMngr));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        };
        List<KAddress> partners = new ArrayList<KAddress>();
        partners.add(senderAdr);
        streamComp = create(StreamHostComp.class, new StreamHostComp.Init(extPorts, selfAdr, torrentDetails, partners));
        connect(streamComp.getPositive(ReportPort.class), reportPort.getPair(), Channel.TWO_WAY);
    }

    public static class StartTransferEvent implements KompicsEvent {

        public final Address senderAdr;
        public final SettableFuture<TransferStats> promise = SettableFuture.create();

        public StartTransferEvent(Address senderAdr) {
            this.senderAdr = senderAdr;
        }
    }

    public static class Init extends se.sics.kompics.Init<Receiver> {

        public final Address selfAdr;
        public final Address senderAdr;
        public final File outputDir;
        public final BlockingQueue<TransferStats> resultQ;

        public Init(Address self, Address senderAdr, File outputDir, BlockingQueue<TransferStats> resultQ) {
            this.selfAdr = self;
            this.senderAdr = senderAdr;
            this.outputDir = outputDir;
            this.resultQ = resultQ;
        }
    }
}
