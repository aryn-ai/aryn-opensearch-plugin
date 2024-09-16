/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.sycamore.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.Ignore;
import org.openapitools.client.ApiClient;
import org.openapitools.client.ApiException;
import org.openapitools.client.Configuration;
import org.openapitools.client.api.DefaultApi;
import org.openapitools.client.auth.HttpBearerAuth;
import org.opensearch.test.OpenSearchTestCase;

import java.io.BufferedReader;
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
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.Base64;

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

    HttpClient client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .followRedirects(HttpClient.Redirect.NORMAL)
        .connectTimeout(Duration.ofSeconds(20))
        //.proxy(ProxySelector.of(new InetSocketAddress("proxy.example.com", 80)))
        //.authenticator(Authenticator.getDefault())
        .build();
    @Ignore
    public void testCallSycamoreEndpoint2() throws Exception {


        byte[] docBytes = FileUtils.readFileToByteArray(Path.of(classLoader.getResource(TEST_DOC_PATH + "lincoln.pdf").toURI()).toFile());
        String docContent = Base64.getEncoder().encodeToString(docBytes);

        String apiToken = System.getenv("ARYN_TOKEN");
        String arynEndpoint = "http://localhost:8000/v1/document/vectorize";

        AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
            JSONObject jsonResponse;
            try {

                HttpRequest request = HttpRequest.newBuilder(URI.create(arynEndpoint))
                    .version(HttpClient.Version.HTTP_1_1)
                    .header("accept", "application/json")
                    .header("Authorization", "Bearer " + apiToken)
                    .POST(HttpRequest.BodyPublishers.ofString(docContent))
                    .build();

                HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());

                String jsonString = response.body().replaceAll("\\\\","");
                jsonResponse = new JSONObject(jsonString);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

            System.out.println("Received updated data:" + jsonResponse);
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
            try {
                Object result = apiInstance.vectorizeV1DocumentVectorizePost(name, docContent);
                System.out.println(result);
            } catch (ApiException e) {
                System.err.println("Exception when calling DefaultApi#vectorizeV1DocumentVectorizePost");
                System.err.println("Status code: " + e.getCode());
                System.err.println("Reason: " + e.getResponseBody());
                System.err.println("Response headers: " + e.getResponseHeaders());
                e.printStackTrace();
            }


            return null;
        });
    }
}
