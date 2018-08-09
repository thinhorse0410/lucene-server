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

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestSuggest extends ServerBaseTestCase {

    static Path tempFile;

    @BeforeClass
    public static void initClass() throws Exception {
        useDefaultIndex = true;
        startServer();
        createAndStartIndex("index");
        commit();
        Path tempDir = createTempDir("TestSuggest");
        Files.createDirectories(tempDir);
        tempFile = tempDir.resolve("suggest.in");
    }

    @AfterClass
    public static void fini() throws Exception {
        shutdownServer();
        tempFile = null;
    }

    public void testAnalyzingSuggest() throws Exception {
        Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("5\u001flucene\u001ffoobar\n");
        out.write("10\u001flucifer\u001ffoobar\n");
        out.write("15\u001flove\u001ffoobar\n");
        out.write("5\u001ftheories take time\u001ffoobar\n");
        out.write("5\u001fthe time is now\u001ffoobar\n");
        out.close();

        JSONObject result = send("buildSuggest", "{source: {localFile: '" + tempFile.toAbsolutePath() + "', hasContexts: false}, class: 'AnalyzingSuggester', suggestName: 'suggest', indexAnalyzer: EnglishAnalyzer, queryAnalyzer: {tokenizer: Standard, tokenFilters: [EnglishPossessive, LowerCase, PorterStem]]}}");
        assertEquals(5, result.get("count"));
        //commit();

        for (int i = 0; i < 2; i++) {
            result = send("suggestLookup", "{text: 'l', suggestName: 'suggest'}");
            assertEquals(3, get(result, "results.length"));

            assertEquals("love", get(result, "results[0].key"));
            assertEquals(15, get(result, "results[0].weight"));
            assertEquals("foobar", get(result, "results[0].payload"));

            assertEquals("lucifer", get(result, "results[1].key"));
            assertEquals(10, get(result, "results[1].weight"));
            assertEquals("foobar", get(result, "results[1].payload"));

            assertEquals("lucene", get(result, "results[2].key"));
            assertEquals(5, get(result, "results[2].weight"));
            assertEquals("foobar", get(result, "results[2].payload"));

            result = send("suggestLookup", "{text: 'the', suggestName: 'suggest'}");
            assertEquals(1, get(result, "results.length"));

            assertEquals("theories take time", get(result, "results[0].key"));
            assertEquals(5, get(result, "results[0].weight"));
            assertEquals("foobar", get(result, "results[0].payload"));

            result = send("suggestLookup", "{text: 'the ', suggestName: 'suggest'}");
            assertEquals(0, get(result, "results.length"));

            // Make sure suggest survives server restart:
            bounceServer();
            send("startIndex");
        }
    }

    public void testInfixSuggest() throws Exception {
        Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("15\u001flove lost\u001ffoobar\n");
        out.close();

        send("buildSuggest", "{source: {localFile: '" + tempFile.toAbsolutePath() + "'}, class: InfixSuggester, suggestName: suggest2, analyzer: {tokenizer: Whitespace, tokenFilters: [LowerCase]}}");
        assertEquals(1, getInt("count"));
        //commit();

        for (int i = 0; i < 2; i++) {
            send("suggestLookup", "{text: lost, suggestName: suggest2}");
            assertEquals(15, getLong("results[0].weight"));
            assertEquals("love <font color=red>lost</font>", toString(getArray("results[0].key")));
            assertEquals("foobar", getString("results[0].payload"));

            send("suggestLookup", "{text: lo, suggestName: suggest2}");
            assertEquals(15, getLong("results[0].weight"));
            assertEquals("<font color=red>lo</font>ve <font color=red>lo</font>st", toString(getArray("results[0].key")));
            assertEquals("foobar", getString("results[0].payload"));

            // Make sure suggest survives server restart:
            bounceServer();
            send("startIndex");
        }
    }

    public void testInfixSuggestNRT() throws Exception {
        Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("15\u001flove lost\u001ffoobar\n");
        out.close();

        send("buildSuggest", "{source: {localFile: '" + tempFile.toAbsolutePath() + "'}, class: InfixSuggester, suggestName: suggestnrt, analyzer: {tokenizer: Whitespace, tokenFilters: [LowerCase]}}");
        assertEquals(1, getInt("count"));

        for (int i = 0; i < 2; i++) {
            //System.out.println("TEST: i=" + i);
            send("suggestLookup", "{text: lost, suggestName: suggestnrt}");
            assertEquals(15, getLong("results[0].weight"));
            assertEquals("love <font color=red>lost</font>", toString(getArray("results[0].key")));
            assertEquals("foobar", getString("results[0].payload"));

            send("suggestLookup", "{text: lo, suggestName: suggestnrt}");
            assertEquals(15, getLong("results[0].weight"));
            assertEquals("<font color=red>lo</font>ve <font color=red>lo</font>st", toString(getArray("results[0].key")));
            assertEquals("foobar", getString("results[0].payload"));

            // Make sure suggest survives server restart:
            bounceServer();
            send("startIndex");
        }

        // Now update the suggestions:
        fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
        out = new BufferedWriter(fstream);
        out.write("10\u001flove lost\u001ffoobaz\n");
        out.write("20\u001flove found\u001ffooboo\n");
        out.close();

        send("updateSuggest", "{source: {localFile: '" + tempFile.toAbsolutePath() + "'}, suggestName: suggestnrt}");
        assertEquals(2, getInt("count"));

        for (int i = 0; i < 2; i++) {
            send("suggestLookup", "{text: lost, suggestName: suggestnrt}");
            assertEquals(10, getLong("results[0].weight"));
            assertEquals("love <font color=red>lost</font>", toString(getArray("results[0].key")));
            assertEquals("foobaz", getString("results[0].payload"));

            send("suggestLookup", "{text: lo, suggestName: suggestnrt}");
            assertEquals(2, getInt("results.length"));
            assertEquals(20, getLong("results[0].weight"));
            assertEquals("<font color=red>lo</font>ve found", toString(getArray("results[0].key")));
            assertEquals("fooboo", getString("results[0].payload"));

            assertEquals(10, getLong("results[1].weight"));
            assertEquals("<font color=red>lo</font>ve <font color=red>lo</font>st", toString(getArray("results[1].key")));
            assertEquals("foobaz", getString("results[1].payload"));

            // Make sure suggest survives server restart:
            bounceServer();
            send("startIndex");
        }
    }

    public String toString(JSONArray fragments) {
        StringBuilder sb = new StringBuilder();
        for (Object _o : fragments) {
            JSONObject o = (JSONObject) _o;
            if ((Boolean) o.get("isHit")) {
                sb.append("<font color=red>");
            }
            sb.append(o.get("text").toString());
            if ((Boolean) o.get("isHit")) {
                sb.append("</font>");
            }
        }
        return sb.toString();
    }

    public void testFuzzySuggest() throws Exception {
        Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("15\u001flove lost\u001ffoobar\n");
        out.close();

        JSONObject result = send("buildSuggest", "{source: {localFile: '" + tempFile.toAbsolutePath() + "', hasContexts: false}, class: 'FuzzySuggester', suggestName: 'suggest3', analyzer: {tokenizer: Whitespace, tokenFilters: [LowerCase]}}");
        assertEquals(1, result.get("count"));
        //commit();

        for (int i = 0; i < 2; i++) {
            // 1 transposition and this is prefix of "love":
            result = send("suggestLookup", "{text: 'lvo', suggestName: 'suggest3'}");
            assertEquals(15, get(result, "results[0].weight"));
            assertEquals("love lost", get(result, "results[0].key"));
            assertEquals("foobar", get(result, "results[0].payload"));

            // Make sure suggest survives server restart:
            bounceServer();
            send("startIndex");
        }
    }

    /**
     * Build a suggest, pulling suggestions/weights/payloads from stored fields.
     */
    public void testFromStoredFields() throws Exception {
        createAndStartIndex("storedsuggest");
        send("registerFields",
                "{fields: {text: {type: text, store: true, search: false}," +
                        "weight: {type: float, store: true, search: false}," +
                        "payload: {type: text, store: true, search: false}}}");
        send("addDocument", "{fields: {text: 'the cat meows', weight: 1, payload: 'payload1'}}");
        long indexGen = getLong(send("addDocument", "{fields: {text: 'the dog barks', weight: 2, payload: 'payload2'}}"), "indexGen");

        JSONObject result = send("buildSuggest", "{source: {searcher: {indexGen: " + indexGen + "}, suggestField: text, weightField: weight, payloadField: payload}, class: 'AnalyzingSuggester', suggestName: 'suggest', analyzer: {tokenizer: Whitespace, tokenFilters: [LowerCase]}}");
        // nocommit count isn't returned for stored fields source:
        //assertEquals(2, result.get("count"));

        for (int i = 0; i < 2; i++) {
            result = send("suggestLookup", "{text: the, suggestName: suggest}");
            assertEquals(2, getInt(result, "results[0].weight"));
            assertEquals("the dog barks", get(result, "results[0].key"));
            assertEquals("payload2", get(result, "results[0].payload"));
            assertEquals(1, getInt(result, "results[1].weight"));
            assertEquals("the cat meows", get(result, "results[1].key"));
            assertEquals("payload1", get(result, "results[1].payload"));

            // Make sure suggest survives server restart:
            bounceServer();
            send("startIndex");
        }
    }

    /**
     * Build a suggest, pulling suggestions/payloads from
     * stored fields, and weight from an expression
     */
    public void testFromStoredFieldsWithWeightExpression() throws Exception {
        createAndStartIndex("storedsuggestexpr");
        send("registerFields",
                "{" +
                        "fields: {text: {type: text, store: true, search: false}," +
                        "negWeight: {type: float, sort: true}," +
                        "payload: {type: text, store: true, search: false}}}");
        send("addDocument", "{fields: {text: 'the cat meows', negWeight: -1, payload: 'payload1'}}");
        long indexGen = getLong(send("addDocument", "{fields: {text: 'the dog barks', negWeight: -2, payload: 'payload2'}}"), "indexGen");

        JSONObject result = send("buildSuggest", "{source: {searcher: {indexGen: " + indexGen + "}, suggestField: text, weightExpression: -negWeight, payloadField: payload}, class: 'AnalyzingSuggester', suggestName: 'suggest', analyzer: {tokenizer: Whitespace, tokenFilters: [LowerCase]}}");
        // nocommit count isn't returned for stored fields source:
        //assertEquals(2, result.get("count"));

        for (int i = 0; i < 2; i++) {
            result = send("suggestLookup", "{text: the, suggestName: suggest}");
            assertEquals(2, getInt(result, "results[0].weight"));
            assertEquals("the dog barks", get(result, "results[0].key"));
            assertEquals("payload2", get(result, "results[0].payload"));
            assertEquals(1, getInt(result, "results[1].weight"));
            assertEquals("the cat meows", get(result, "results[1].key"));
            assertEquals("payload1", get(result, "results[1].payload"));

            // Make sure suggest survives server restart:
            bounceServer();
            send("startIndex");
        }
    }

    public void testInfixSuggesterWithContexts() throws Exception {
        createAndStartIndex();

        Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
        BufferedWriter out = new BufferedWriter(fstream);
        out.write("15\u001flove lost\u001ffoobar\u001flucene\n");
        out.close();

        send("buildSuggest", "{source: {localFile: '" + tempFile.toAbsolutePath() + "'}, class: 'InfixSuggester', suggestName: 'infix_suggest', analyzer: {tokenizer: Whitespace, tokenFilters: [LowerCase]}}");
        assertEquals(1, getInt("count"));

        for (int i = 0; i < 2; i++) {
            send("suggestLookup", "{text: 'lov', suggestName: 'infix_suggest', contexts: ['lucene']}");
            assertEquals(1, getInt("results.length"));
            assertEquals(15, getInt("results[0].weight"));
            assertEquals("<font color=red>lov</font>e lost", toString(getArray("results[0].key")));
            assertEquals("foobar", getString("results[0].payload"));

            // Make sure suggest survives server restart:
            bounceServer();
            send("startIndex");
        }
    }

    // nocommit test full build over an already built suggester
}
