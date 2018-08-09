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
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;

import java.util.List;
import java.util.Map;

/**
 * Handles {@code indexStatus}.
 */
public class IndexStatusHandler extends Handler {
    private static StructType TYPE = new StructType(
            new Param("indexName", "Index name", new StringType()));

    @Override
    public StructType getType() {
        return TYPE;
    }

    @Override
    public String getTopDoc() {
        return "Returns index status (started or stopped)";
    }

    /**
     * Sole constructor.
     */
    public IndexStatusHandler(GlobalState state) {
        super(state);
    }

    @Override
    public FinishRequest handle(final IndexState state, final Request r, Map<String, List<String>> params) throws Exception {

        return new FinishRequest() {
            @Override
            public String finish() throws Exception {
                JSONObject result = new JSONObject();
                result.put("status", state.isStarted() ? "started" : "stopped");
                return result.toString();
            }
        };
    }
}
