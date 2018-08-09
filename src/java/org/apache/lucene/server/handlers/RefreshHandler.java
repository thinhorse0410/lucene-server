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
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;

import java.util.List;
import java.util.Map;

/**
 * Handles {@code refresh}.
 */
public class RefreshHandler extends Handler {
    private static StructType TYPE = new StructType(
            new Param("indexName", "Index name", new StringType()));

    @Override
    public StructType getType() {
        return TYPE;
    }

    @Override
    public String getTopDoc() {
        return "Refresh the latest searcher for an index";
    }

    /**
     * Sole constructor.
     */
    public RefreshHandler(GlobalState state) {
        super(state);
    }

    @Override
    public FinishRequest handle(final IndexState indexState, final Request r, Map<String, List<String>> params) throws Exception {
        final ShardState shardState = indexState.getShard(0);
        return new FinishRequest() {
            @Override
            public String finish() throws Exception {
                long t0 = System.nanoTime();
                JSONObject result = new JSONObject();
                shardState.maybeRefreshBlocking();
                long t1 = System.nanoTime();
                result.put("refreshTimeMS", ((t1 - t0) / 1000000.0));
                return result.toString();
            }
        };
    }
}
