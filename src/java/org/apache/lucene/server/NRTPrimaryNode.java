package org.apache.lucene.server;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.replicator.nrt.FileMetaData;
import org.apache.lucene.replicator.nrt.PrimaryNode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.server.handlers.CopyFilesHandler;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.*;

public class NRTPrimaryNode extends PrimaryNode {

    public int[] replicaIDs = new int[0];
    public InetSocketAddress[] replicaAddresses = new InetSocketAddress[0];
    private final InetSocketAddress localAddress;
    final String indexName;

    final List<MergePreCopy> warmingSegments = Collections.synchronizedList(new ArrayList<>());

    public NRTPrimaryNode(String indexName, InetSocketAddress localAddress, IndexWriter writer, int id, long primaryGen, long forcePrimaryVersion,
                          SearcherFactory searcherFactory, PrintStream printStream) throws IOException {
        super(writer, id, primaryGen, forcePrimaryVersion, searcherFactory, printStream);
        this.localAddress = localAddress;
        this.indexName = indexName;
    }

    /**
     * Holds all replicas currently warming (pre-copying the new files) a single merged segment
     */
    static class MergePreCopy {
        final List<Connection> connections = Collections.synchronizedList(new ArrayList<>());
        final Map<String, FileMetaData> files;
        private boolean finished;

        public MergePreCopy(Map<String, FileMetaData> files) {
            this.files = files;
        }

        public synchronized boolean tryAddConnection(Connection c) {
            if (finished == false) {
                connections.add(c);
                return true;
            } else {
                return false;
            }
        }

        public synchronized boolean finished() {
            if (connections.isEmpty()) {
                finished = true;
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    protected void preCopyMergedSegmentFiles(SegmentCommitInfo info, Map<String, FileMetaData> files) throws IOException {

        InetSocketAddress[] replicaAddresses = this.replicaAddresses;
        if (replicaAddresses == null) {
            message("no replicas; skip warming " + info);
            return;
        }

        message("top: warm merge " + info + " to " + replicaAddresses.length + " replicas; localAddress=" + localAddress + ": files=" + files.keySet());

        MergePreCopy preCopy = new MergePreCopy(files);
        warmingSegments.add(preCopy);

        try {

            // Ask all currently known replicas to pre-copy this newly merged segment's files:
            for (InetSocketAddress replicaAddress : replicaAddresses) {
                try {
                    Connection c = new Connection(replicaAddress);
                    c.out.writeInt(Server.BINARY_MAGIC);
                    c.out.writeString("copyFiles");
                    c.out.writeString(indexName);
                    c.out.writeVLong(primaryGen);
                    CopyFilesHandler.writeFilesMetaData(c.out, files);
                    c.flush();
                    c.s.shutdownOutput();
                    message("warm connection " + c.s);
                    preCopy.connections.add(c);
                } catch (Throwable t) {
                    message("top: ignore exception trying to warm to replica port " + replicaAddress + ": " + t);
                    //t.printStackTrace(System.out);
                }
            }

            long startNS = System.nanoTime();
            long lastWarnNS = startNS;

            // TODO: maybe ... place some sort of time limit on how long we are willing to wait for slow replica(s) to finish copying?
            while (preCopy.finished() == false) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                    throw new ThreadInterruptedException(ie);
                }

                if (isClosed()) {
                    message("top: primary is closing: now cancel segment warming");
                    synchronized (preCopy.connections) {
                        IOUtils.closeWhileHandlingException(preCopy.connections);
                    }
                    return;
                }

                long ns = System.nanoTime();
                if (ns - lastWarnNS > 1000000000L) {
                    message(String.format(Locale.ROOT, "top: warning: still warming merge " + info + " to " + preCopy.connections.size() + " replicas for %.1f sec...", (ns - startNS) / 1000000000.0));
                    lastWarnNS = ns;
                }

                // Because a replica can suddenly start up and "join" into this merge pre-copy:
                synchronized (preCopy.connections) {
                    Iterator<Connection> it = preCopy.connections.iterator();
                    while (it.hasNext()) {
                        Connection c = it.next();
                        try {
                            long nowNS = System.nanoTime();
                            boolean done = false;
                            // nocommit find a way not to use available here:
                            while (c.sockIn.available() > 0) {
                                byte b = c.in.readByte();
                                if (b == 0) {
                                    // keep-alive
                                    c.lastKeepAliveNS = nowNS;
                                    message("keep-alive for socket=" + c.s + " merge files=" + files.keySet());
                                } else {
                                    // merge is done pre-copying to this node
                                    if (b != 1) {
                                        throw new IllegalArgumentException();
                                    }
                                    message("connection socket=" + c.s + " is done warming its merge " + info + " files=" + files.keySet());
                                    IOUtils.closeWhileHandlingException(c);
                                    it.remove();
                                    done = true;
                                    break;
                                }
                            }
                        } catch (Throwable t) {
                            message("top: ignore exception trying to read byte during warm for segment=" + info + " to replica socket=" + c.s + ": " + t + " files=" + files.keySet());
                            IOUtils.closeWhileHandlingException(c);
                            it.remove();
                        }
                    }
                }

                // TODO

                // Process keep-alives:
        /*
        synchronized(preCopy.connections) {
          Iterator<Connection> it = preCopy.connections.iterator();
          while (it.hasNext()) {
            Connection c = it.next();
            try {
              long nowNS = System.nanoTime();
              boolean done = false;
              while (c.sockIn.available() > 0) {
                byte b = c.in.readByte();
                if (b == 0) {
                  // keep-alive
                  c.lastKeepAliveNS = nowNS;
                  message("keep-alive for socket=" + c.s + " merge files=" + files.keySet());
                } else {
                  // merge is done pre-copying to this node
                  if (b != 1) {
                    throw new IllegalArgumentException();
                  }
                  message("connection socket=" + c.s + " is done warming its merge " + info + " files=" + files.keySet());
                  IOUtils.closeWhileHandlingException(c);
                  it.remove();
                  done = true;
                  break;
                }
              }

              // If > 2 sec since we saw a keep-alive, assume this replica is dead:
              if (done == false && nowNS - c.lastKeepAliveNS > 2000000000L) {
                message("top: warning: replica socket=" + c.s + " for segment=" + info + " seems to be dead; closing files=" + files.keySet());
                IOUtils.closeWhileHandlingException(c);
                it.remove();
                done = true;
              }

              if (done == false && random.nextInt(1000) == 17) {
                message("top: warning: now randomly dropping replica from merge warming; files=" + files.keySet());
                IOUtils.closeWhileHandlingException(c);
                it.remove();
                done = true;
              }

            } catch (Throwable t) {
              message("top: ignore exception trying to read byte during warm for segment=" + info + " to replica socket=" + c.s + ": " + t + " files=" + files.keySet());
              IOUtils.closeWhileHandlingException(c);
              it.remove();
            }
          }
        }
        */
            }
            message("top: done warming merge " + info);
        } finally {
            warmingSegments.remove(preCopy);
        }
    }

    /**
     * Records currently alive replicas.
     */
    public synchronized void setReplicas(int[] replicaIDs, InetSocketAddress[] replicaAddresses) {
        message("top: set replicasIDs=" + Arrays.toString(replicaIDs) + " addresses=" + Arrays.toString(replicaAddresses));
        this.replicaIDs = replicaIDs;
        this.replicaAddresses = replicaAddresses;
    }

    void sendNewNRTPointToReplicas() {
        System.out.println("NRTPrimaryNode: sendNRTPoint");
        // Something did get flushed (there were indexing ops since the last flush):

        // nocommit: we used to notify caller of the version, before trying to push to replicas, in case we crash after flushing but
        // before notifying all replicas, at which point we have a newer version index than client knew about?
        long version = getCopyStateVersion();

        int[] replicaIDs;
        InetSocketAddress[] replicaAddresses;
        synchronized (this) {
            replicaIDs = this.replicaIDs;
            replicaAddresses = this.replicaAddresses;
        }

        message("send flushed version=" + version + " replica count " + replicaIDs.length);

        // Notify current replicas:
        for (int i = 0; i < replicaIDs.length; i++) {
            int replicaID = replicaIDs[i];
            try (Connection c = new Connection(replicaAddresses[i])) {
                message("send NEW_NRT_POINT to R" + replicaID + " at address=" + replicaAddresses[i]);
                c.out.writeInt(Server.BINARY_MAGIC);
                c.out.writeString("newNRTPoint");
                c.out.writeString(indexName);
                c.out.writeVLong(version);
                c.out.writeVLong(primaryGen);
                c.flush();
                // TODO: we should use multicast to broadcast files out to replicas
                // TODO: ... replicas could copy from one another instead of just primary
                // TODO: we could also prioritize one replica at a time?
            } catch (Throwable t) {
                message("top: failed to connect R" + replicaID + " for newNRTPoint; skipping: " + t.getMessage());
            }
        }
    }

    // TODO: awkward we are forced to do this here ... this should really live in replicator code, e.g. PrimaryNode.mgr should be this:
    static class PrimaryNodeReferenceManager extends ReferenceManager<IndexSearcher> {
        final NRTPrimaryNode primary;
        final SearcherFactory searcherFactory;

        public PrimaryNodeReferenceManager(NRTPrimaryNode primary, SearcherFactory searcherFactory) throws IOException {
            this.primary = primary;
            this.searcherFactory = searcherFactory;
            current = SearcherManager.getSearcher(searcherFactory, primary.mgr.acquire().getIndexReader(), null);
        }

        @Override
        protected void decRef(IndexSearcher reference) throws IOException {
            reference.getIndexReader().decRef();
        }

        @Override
        protected IndexSearcher refreshIfNeeded(IndexSearcher referenceToRefresh) throws IOException {
            if (primary.flushAndRefresh()) {
                primary.sendNewNRTPointToReplicas();
                // NOTE: steals a ref from one ReferenceManager to another!
                return SearcherManager.getSearcher(searcherFactory, primary.mgr.acquire().getIndexReader(), referenceToRefresh.getIndexReader());
            } else {
                return null;
            }
        }

        @Override
        protected boolean tryIncRef(IndexSearcher reference) {
            return reference.getIndexReader().tryIncRef();
        }

        @Override
        protected int getRefCount(IndexSearcher reference) {
            return reference.getIndexReader().getRefCount();
        }
    }

    public void addReplica(int replicaID, InetSocketAddress replicaAddress) throws IOException {

        message("add replica: " + warmingSegments.size() + " current warming merges");

        // nocommit switch to List<>:
        synchronized (this) {
            int count = this.replicaIDs.length + 1;
            int[] replicaIDs = new int[count];
            System.arraycopy(this.replicaIDs, 0, replicaIDs, 0, count - 1);
            this.replicaIDs = replicaIDs;
            replicaIDs[count - 1] = replicaID;
            InetSocketAddress[] replicaAddresses = new InetSocketAddress[count];
            System.arraycopy(this.replicaAddresses, 0, replicaAddresses, 0, count - 1);
            replicaAddresses[count - 1] = replicaAddress;
            this.replicaAddresses = replicaAddresses;
        }

        // Step through all currently warming segments and try to add this replica if it isn't there already:
        synchronized (warmingSegments) {
            for (MergePreCopy preCopy : warmingSegments) {
                message("warming segment " + preCopy.files.keySet());
                boolean found = false;
                synchronized (preCopy.connections) {
                    for (Connection c : preCopy.connections) {
                        if (c.destAddress == replicaAddress) {
                            found = true;
                            break;
                        }
                    }
                }

                if (found) {
                    message("this replica is already warming this segment; skipping");
                    // It's possible (maybe) that the replica started up, then a merge kicked off, and it warmed to this new replica, all before the
                    // replica sent us this command:
                    continue;
                }

                // OK, this new replica is not already warming this segment, so attempt (could fail) to start warming now:

                Connection c = new Connection(replicaAddress);
                if (preCopy.tryAddConnection(c) == false) {
                    // This can happen, if all other replicas just now finished warming this segment, and so we were just a bit too late.  In this
                    // case the segment must be copied over in the next nrt point sent to this replica
                    message("failed to add connection to segment warmer (too late); closing");
                    c.close();
                }

                c.out.writeInt(Server.BINARY_MAGIC);
                c.out.writeString("copyFiles");
                c.out.writeString(indexName);
                c.out.writeVLong(primaryGen);
                CopyFilesHandler.writeFilesMetaData(c.out, preCopy.files);
                c.flush();
                c.s.shutdownOutput();
                message("successfully started warming");
            }
        }
    }

    public void setRAMBufferSizeMB(double mb) {
        writer.getConfig().setRAMBufferSizeMB(mb);
    }
}
