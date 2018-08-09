package org.apache.lucene.server.handlers;

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

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.QueryID;
import org.apache.lucene.server.params.*;

import java.io.IOException;
import java.util.*;

// nocommit why no double range faceting?

// nocommit remove SearchHandler and replace w/ this impl

/**
 * Handles {@code search} using distributed queue.
 */
public class Search2Handler extends Handler {

    private final static StructType TYPE =
            new StructType(
                    new Param("queryText", "Query text to parse using the specified QueryParser.", new StringType()),
                    new Param("indexNames", "Which indices to query.", new ListType(new StringType()))
            );

    @Override
    public String getTopDoc() {
        return "Execute a search.";
    }

    @Override
    public StructType getType() {
        return TYPE;
    }

    /**
     * Sole constructor.
     */
    public Search2Handler(GlobalState state) {
        super(state);
        requiresIndexName = false;
    }

    /**
     * Holds returned results from one index
     */
    private static class QueryHits {
        public final QueryID queryID;
        public final TopDocs hits;

        public QueryHits(QueryID queryID, TopDocs hits) {
            this.queryID = queryID;
            this.hits = hits;
        }
    }

    /**
     * Returned hits are added here
     */
    private final Map<QueryID, QueryHits> pendingHits = new HashMap<>();

    public synchronized void deliverHits(QueryID queryID, TopDocs hits) {
        pendingHits.put(queryID, new QueryHits(queryID, hits));
        notifyAll();
    }

    @Override
    public FinishRequest handle(final IndexState ignored, final Request r, Map<String, List<String>> params) throws Exception {

        String queryText = r.getString("queryText");

        final Set<QueryID> queryIDs = new HashSet<>();

        long t0 = System.nanoTime();

        for (Object _indexName : r.getList("indexNames")) {
            String indexName = (String) _indexName;
            IndexState indexState = globalState.getIndex(indexName);

            // Enroll the query in the distributed queue:
            for (Integer shardOrd : indexState.shards.keySet()) {
                // Assign a unique ID for this query + index + shard
                QueryID queryID = new QueryID();
                queryIDs.add(queryID);
                globalState.searchQueue.addNewQuery(queryID, indexName, shardOrd, queryText, globalState.nodeID);
            }

            // nocommit move this into addNewQuery
      /*
      for(RemoteNodeConnection node : globalState.remoteNodes) {
        synchronized(node.c) {
          node.c.out.writeByte(NodeToNodeHandler.CMD_NEW_QUERY);
          node.c.out.writeBytes(queryID.id, 0, queryID.id.length);
          // nocommit must send shard too:
          node.c.out.writeString(indexName);
          node.c.out.writeString(queryText);
          node.c.flush();
        }
      }
      */
        }

        // TODO: we could let "wait for results" be optional here:

        // Wait for hits to come back:
        final List<QueryHits> allHits = new ArrayList<>();
        synchronized (this) {
            while (true) {
                Iterator<QueryID> it = queryIDs.iterator();
                while (it.hasNext()) {
                    QueryID queryID = it.next();
                    QueryHits hits = pendingHits.remove(queryID);
                    if (hits != null) {
                        allHits.add(hits);
                        it.remove();
                    }
                }
                if (queryIDs.size() == 0) {
                    break;
                }
                wait();
            }
        }

        final double msec = (System.nanoTime() - t0) / 1000000.0;

        TopDocs[] allTopDocs = new TopDocs[allHits.size()];
        for (int i = 0; i < allTopDocs.length; i++) {
            allTopDocs[i] = allHits.get(i).hits;
        }
        final TopDocs mergedHits = TopDocs.merge(10, allTopDocs);

        return new FinishRequest() {
            @Override
            public String finish() throws IOException {
                JSONObject result = new JSONObject();
                //result.put("queryID", queryID);
                result.put("queryTimeMS", msec);
                result.put("totalHits", mergedHits.totalHits);
                JSONArray hitsArray = new JSONArray();
                for (ScoreDoc hit : mergedHits.scoreDocs) {
                    JSONObject hitObj = new JSONObject();
                    hitObj.put("docID", hit.doc);
                    hitObj.put("score", hit.score);
                    hitsArray.add(hitObj);
                }
                result.put("hits", hitsArray);
                return result.toString();
            }
        };
    }
}
