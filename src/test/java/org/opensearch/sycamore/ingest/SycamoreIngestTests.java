/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.sycamore.ingest;

import aryn.partitioner.ApiClient;
import aryn.partitioner.ApiException;
import aryn.partitioner.Configuration;
import aryn.partitioner.api.DefaultApi;
import aryn.partitioner.auth.HttpBearerAuth;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.Ignore;
import org.opensearch.core.action.ActionListener;
import org.opensearch.searchpipelines.questionanswering.generative.llm.*;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.net.http.HttpClient.newHttpClient;

public class SycamoreIngestTests extends OpenSearchTestCase {
    private static final String TEST_DOC_PATH = "org/opensearch/sycamore/ingest/test_data/";
    protected ClassLoader classLoader = SycamoreIngestTests.class.getClassLoader();

    // Add unit tests for your plugin
    @Ignore
    public void testCallSycamoreEndpoint() throws Exception {

        byte[] docBytes = FileUtils.readFileToByteArray(Path.of(classLoader.getResource(TEST_DOC_PATH + "lincoln.pdf").toURI()).toFile());
        String docContent = Base64.getEncoder().encodeToString(docBytes);

        String apiToken = System.getenv("ARYN_TOKEN");
        String arynEndpoint = "http://localhost:8000/v1/document/vectorize";

        AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
            URL url = new URL(arynEndpoint);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            String contentType = "application/json";
            conn.setRequestProperty("Content-Type", contentType);
            conn.setRequestProperty("Accept", contentType);
            conn.setRequestProperty("Authorization", "Bearer " + apiToken);
            conn.setDoOutput(true);
            String filename = "test";
            JSONObject data = new JSONObject();
            data.put("name", filename);
            data.put("data", docContent);

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonInputString = String.format("{\"name\": \"%s\", \"data\": \"%s\"}", filename, docContent);
            // String jsonInputString = data.toString(); // URLEncoder.encode(objectMapper.writeValueAsString(data), StandardCharsets.UTF_8);
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String responseLine = null;
                while ((responseLine = br.readLine()) != null) {
                    response.append(responseLine.trim());
                }
                System.out.println(conn.getResponseCode() + " " + response);
            }

            return null;
        });
    }

    public void testCallSycamoreEndpoint3() throws Exception {

        byte[] docBytes = FileUtils.readFileToByteArray(Path.of(classLoader.getResource(TEST_DOC_PATH + "lincoln.pdf").toURI()).toFile());
        String docContent = Base64.getEncoder().encodeToString(docBytes);

        String apiToken = System.getenv("ARYN_TOKEN");
        String arynEndpoint = "http://localhost:8000/v1/document/vectorize";

        AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
            ApiClient defaultClient = Configuration.getDefaultApiClient();
            defaultClient.setBasePath("http://localhost:8000");

            // Configure HTTP bearer authorization: HTTPBearer
            HttpBearerAuth HTTPBearer = (HttpBearerAuth) defaultClient.getAuthentication("HTTPBearer");
            HTTPBearer.setBearerToken(apiToken);

            DefaultApi apiInstance = new DefaultApi(defaultClient);
            String name = "lincoln.pdf"; // String |
            // String |

            /*
            try {
                // Object result = apiInstance.vectorizeV1DocumentVectorizePost(name, docContent);
                // System.out.println(result);
            } catch (ApiException e) {
                System.err.println("Exception when calling DefaultApi#vectorizeV1DocumentVectorizePost");
                System.err.println("Status code: " + e.getCode());
                System.err.println("Reason: " + e.getResponseBody());
                System.err.println("Response headers: " + e.getResponseHeaders());
                e.printStackTrace();
            }*/

            return null;
        });
    }

    class Holder {
        public String answer;
    }

    public void testCallAPS() throws Exception {

        String apiToken = System.getenv("ARYN_TOKEN");

        AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
            ApiClient defaultClient = Configuration.getDefaultApiClient();
            defaultClient.setBasePath("https://api.aryn.cloud");

            // Configure HTTP bearer authorization: HTTPBearer
            HttpBearerAuth HTTPBearer = (HttpBearerAuth) defaultClient.getAuthentication("HTTPBearer");
            HTTPBearer.setBearerToken(apiToken);

            DefaultApi apiInstance = new DefaultApi(defaultClient);

            Object elements = null;
            try {
                File pdf = Paths.get("/home/austin/national_parks_images_table.pdf").toFile();  // Path.of(classLoader.getResource(TEST_DOC_PATH + "lincoln.pdf").toURI()).toFile();
                File options = Paths.get("/home/austin/aps_options.json").toFile();
                String userAgent = "SycamoreIngestPlugin_v0.1.0";
                Object result = apiInstance.partitionPdfAsyncV1DocumentPartitionPost(pdf, userAgent, options);
                // System.out.println(result);
                assert result instanceof Map;
                elements = ((Map) result).get("elements");
                assert elements != null;
                assert elements instanceof List;
                for (Object element : ((List) elements)) {
                    assert element instanceof Map;
                    Map map = (Map) element;
                    if (map.containsKey("text_representation")) {
                        System.out.println(map.get("text_representation"));
                    }
                }

                final Holder holder = new Holder();
                Llm llm = new DefaultLlmImpl("", new NoOpNodeClient(""));
                ChatCompletionInput input = LlmIOUtil.createChatCompletionInput("", "", "", "", Collections.emptyList(), Collections.emptyList(), 30, "", Collections.emptyList());
                llm.doChatCompletion(input, new ActionListener<ChatCompletionOutput>() {
                    @Override
                    public void onResponse(ChatCompletionOutput chatCompletionOutput) {
                        holder.answer = (String) chatCompletionOutput.getAnswers().get(0);
                    }

                    @Override
                    public void onFailure(Exception e) {

                    }
                });

                System.out.println("Answer: " + holder.answer);

            } catch (ApiException e) {
                System.err.println("Exception when calling /v1/document/partition");
                System.err.println("Status code: " + e.getCode());
                System.err.println("Reason: " + e.getResponseBody());
                System.err.println("Response headers: " + e.getResponseHeaders());
                e.printStackTrace();
            }

            return null;
        });
    }

}
