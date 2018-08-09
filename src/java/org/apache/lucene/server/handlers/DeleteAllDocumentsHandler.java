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

import net.minidev.json.JSONObject;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.ShardState;
import org.apache.lucene.server.params.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Handles {@code deleteAllDocuments}.
 */
public class DeleteAllDocumentsHandler extends Handler {

    private final static StructType TYPE =
            new StructType(new Param("indexName", "Which index to search", new StringType()));

    @Override
    public StructType getType() {
        return TYPE;
    }

    @Override
    public String getTopDoc() {
        return "Removes all documents in the index, but keeps all registered fields, settings and any built suggesters.";
    }

    /**
     * Sole constructor.
     */
    public DeleteAllDocumentsHandler(GlobalState state) {
        super(state);
    }

    @Override
    public FinishRequest handle(final IndexState indexState, Request r, Map<String, List<String>> params) throws Exception {
        indexState.verifyStarted(r);
        ShardState shardState = indexState.getShard(0);
        return new FinishRequest() {
            @Override
            public String finish() throws IOException {
                // nocommit should also somehow reset taxo index?
                long gen = shardState.writer.deleteAll();
                JSONObject r = new JSONObject();
                r.put("indexGen", gen);
                return r.toString();
            }
        };
    }
}
