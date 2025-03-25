package com.krrr.dify.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.util.Assert;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.ResourceAccessException;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class serves as a client for interacting with the Dify API.
 * It provides methods for sending various types of requests to the API.
 */
public class DifyClient {
    public static final String OFFICIAL_API_URL = "https://api.dify.ai/v1";

    // Constants representing different API routes
    public static final DifyRoute APPLICATION = new DifyRoute("GET", "/parameters?user=%s");
    public static final DifyRoute FEEDBACK = new DifyRoute("POST", "/messages/%s/feedbacks");
    public static final DifyRoute STOP_GENERATION = new DifyRoute("POST", "/chat-messages/%s/stop");
    public static final DifyRoute CREATE_CHAT_MESSAGE = new DifyRoute("POST", "/chat-messages");
    public static final DifyRoute GET_CONVERSATION_MESSAGES = new DifyRoute("GET", "/messages?%s");
    public static final DifyRoute GET_CONVERSATIONS = new DifyRoute("GET", "/conversations?%s");
    public static final DifyRoute RENAME_CONVERSATION = new DifyRoute("POST", "/conversations/%s/name");
    public static final DifyRoute DELETE_CONVERSATION = new DifyRoute("DELETE", "/conversations/%s");

    private String apiKey;
    private final String baseUrl;
    private RestTemplate restTemplate;
    private StreamRestTemplate streamRestTemplate;
    protected final ObjectMapper objectMapper;
    public boolean autoGenerateName = false;
    public static long connectTimeout = 15L;
    public static long readTimeout = 30L;
    public static final ExecutorService executor = Executors.newCachedThreadPool();


    /**
     * Constructs a new DifyClient with the provided API key and default base URL.
     *
     * @param apiKey The API key to use for authentication.
     */
    public DifyClient(String apiKey) {
        this(apiKey, OFFICIAL_API_URL);
    }

    /**
     * Constructs a new DifyClient with the provided API key and base URL.
     *
     * @param apiKey   The API key to use for authentication.
     * @param baseUrl  The base URL of the Dify API.
     */
    public DifyClient(String apiKey, String baseUrl) {
        if (apiKey == null) {
            throw new IllegalArgumentException("key is null");
        }
        this.apiKey = apiKey;
        this.baseUrl = baseUrl;
        RestTemplateBuilder builder = new RestTemplateBuilder()
                .setConnectTimeout(Duration.ofSeconds(connectTimeout)).setReadTimeout(Duration.ofSeconds(readTimeout));
        restTemplate = builder.build();
        streamRestTemplate = builder.build(StreamRestTemplate.class);
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Updates the API key used for authentication.
     *
     * @param apiKey The new API key.
     */
    public void updateApiKey(String apiKey) {
        this.apiKey = apiKey;
    }

    /**
     * Sends an HTTP request to the Dify API.
     *
     * @param route      The API route to send the request to.
     * @param formatArgs Format arguments for route URL placeholders.
     * @param body       The request body, if applicable.
     * @return The HTTP response containing the result of the API request.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public String sendRequest(DifyRoute route, String[] formatArgs, JsonNode body) throws DifyClientException {
        try {
            String formattedURL = (formatArgs != null && formatArgs.length > 0)
                    ? String.format(route.url, (Object[]) formatArgs)
                    : route.url;

            HttpHeaders headers = new HttpHeaders();
            headers.set(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> entity = body != null
                    ? new HttpEntity<>(objectMapper.writeValueAsString(body), headers)
                    : new HttpEntity<>(headers);

            ResponseEntity<String> resp = restTemplate.exchange(baseUrl + formattedURL, HttpMethod.valueOf(route.method), entity, String.class);

            if (resp.getStatusCode() != HttpStatus.OK) {
                throw new DifyRequestException("Request failed with status: " + resp.getStatusCodeValue());
            }
            return resp.getBody();
        } catch (IOException | ResourceAccessException | HttpClientErrorException e) {
            throw new DifyRequestException("Error occurred while sending request: " + e.getMessage(), e);
        }
    }

    /**
     * Sends an HTTP request to the Dify API.
     *
     * @param route      The API route to send the request to.
     * @param formatArgs Format arguments for route URL placeholders.
     * @param body       The request body, if applicable.
     * @return The HTTP response containing the result of the API request.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public PipedInputStream sendRequestStream(DifyRoute route, String[] formatArgs, JsonNode body) throws DifyClientException {
        try {
            String formattedURL = (formatArgs != null && formatArgs.length > 0)
                    ? String.format(route.url, (Object[]) formatArgs)
                    : route.url;

            HttpHeaders headers = new HttpHeaders();
            headers.set(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> entity = body != null
                    ? new HttpEntity<>(objectMapper.writeValueAsString(body), headers)
                    : new HttpEntity<>(headers);

            ResponseEntity<InputStreamResource> resp = streamRestTemplate.exchange(baseUrl + formattedURL, HttpMethod.valueOf(route.method), entity, InputStreamResource.class);
            InputStream stream = resp.getBody().getInputStream();
            PipedOutputStream output = new PipedOutputStream();
            PipedInputStream ret = new PipedInputStream(output);
            executor.execute(() -> parseSSE(stream, output));
            return ret;
        } catch (IOException | ResourceAccessException | HttpClientErrorException e) {
            throw new DifyRequestException("Error occurred while sending request: " + e.getMessage(), e);
        }
    }

    /**
     * Stops the generation of a chat message.
     *
     * @param taskId Task ID, which can be obtained from the streaming returned Chunk.
     * @param user   The user associated with the chat message.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public void stopGeneration(String taskId, String user) throws DifyClientException {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("user", user);

        JsonNode ret;
        try {
            ret = objectMapper.readTree(sendRequest(STOP_GENERATION, new String[]{taskId}, json));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
        Assert.isTrue("success".equals(ret.get("result").textValue()), "unknown result");
    }

    /**
     * Sends a message feedback to the Dify API.
     *
     * @param messageId The ID of the message to provide feedback for.
     * @param rating    The feedback rating.
     * @param user      The user providing the feedback.
     * @return The HTTP response containing the result of the API request.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public JsonNode messageFeedback(String messageId, String rating, String user) throws DifyClientException {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("rating", rating);
        json.put("user", user);

//        return sendRequest(FEEDBACK, new String[]{messageId}, json);
        return null;
    }

    /**
     * Retrieves application parameters from the Dify API.
     *
     * @param user The user for whom the application parameters are retrieved.
     * @return The HTTP response containing the result of the API request.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public JsonNode getApplicationParameters(String user) throws DifyClientException {
        return null;
//        return sendRequest(APPLICATION, new String[]{user}, null);
    }

    /**
     * Generates query parameters in the form of key-value pairs joined by "&".
     *
     * @param params The map of query parameter key-value pairs.
     * @return A string representation of the generated query parameters.
     */
    private String generateQueryParams(Map<String, String> params) {
        List<String> keyValuePairs = new ArrayList<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            keyValuePairs.add(entry.getKey() + "=" + entry.getValue());
        }
        return String.join("&", keyValuePairs);
    }

    /**
     * Creates a new chat message.
     *
     * @param inputs         The chat message inputs.
     * @param query          The query associated with the chat message.
     * @param user           The user associated with the chat message.
     * @param conversation_id The ID of the conversation, if applicable.
     * @return The HTTP response containing the result of the API request.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public Message createChatMessage(JsonNode inputs, String query, String user, String conversation_id) throws DifyClientException {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("inputs", inputs);
        json.put("query", query);
        json.put("user", user);
        json.put("response_mode", "blocking");
        json.put("auto_generate_name", autoGenerateName);
        if (conversation_id != null && !conversation_id.isEmpty()) {
            json.put("conversation_id", conversation_id);
        }

        String ret = sendRequest(CREATE_CHAT_MESSAGE, null, json);
        try {
            return objectMapper.readValue(ret, new TypeReference<Message>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    public PipedInputStream createChatMessageStream(JsonNode inputs, String query, String user, String conversation_id) throws DifyClientException {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("inputs", inputs);
        json.put("query", query);
        json.put("user", user);
        json.put("response_mode", "streaming");
        json.put("auto_generate_name", autoGenerateName);
        if (conversation_id != null && !conversation_id.isEmpty()) {
            json.put("conversation_id", conversation_id);
        }

        return sendRequestStream(CREATE_CHAT_MESSAGE, null, json);
    }

    /**
     * Retrieves conversation messages.
     *
     * @param user           The user associated with the conversation.
     * @param conversation_id The ID of the conversation.
     * @param first_id       The ID of the first message to start fetching from.
     * @param limit          The maximum number of messages to retrieve.
     * @return The HTTP response containing the result of the API request.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public MessageList getConversationMessages(String user, String conversation_id, String first_id, Integer limit) throws DifyClientException {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("user", user);

        if (conversation_id != null) {
            queryParams.put("conversation_id", conversation_id);
        }
        if (first_id != null) {
            queryParams.put("first_id", first_id);
        }
        if (limit != null) {
            queryParams.put("limit", String.valueOf(limit));
        }
        String formattedQueryParams = generateQueryParams(queryParams);

        String json = sendRequest(GET_CONVERSATION_MESSAGES, new String[] {formattedQueryParams}, null);
        try {
            return objectMapper.readValue(json, MessageList.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Retrieves conversations.
     *
     * @param user       The user associated with the conversations.
     * @param last_id    The ID of the last record on the current page, default is null.
     * @param limit      The maximum number of conversations to retrieve.
     * @param sortBy     The sorting criteria for the conversations.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public ConversationList getConversations(String user, String last_id, Integer limit, String sortBy) throws DifyClientException {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("user", user);
        if (last_id != null && !last_id.isEmpty()) {
            queryParams.put("last_id", last_id);
        }
        if (limit != null) {
            queryParams.put("limit", String.valueOf(limit));
        }
        if (sortBy != null && !sortBy.isEmpty()) {
            queryParams.put("sort_by", sortBy);
        }
        String formattedQueryParams = generateQueryParams(queryParams);
        String json = sendRequest(GET_CONVERSATIONS, new String[] {formattedQueryParams}, null);
        try {
            return objectMapper.readValue(json, ConversationList.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Renames a conversation.
     *
     * @param conversation_id The ID of the conversation to rename.
     * @param name            The new name for the conversation.
     * @param user            The user associated with the conversation.
     * @return The HTTP response containing the result of the API request.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public JsonNode renameConversation(String conversation_id, String name, String user) throws DifyClientException {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("name", name);
        json.put("user", user);

        String ret = sendRequest(RENAME_CONVERSATION, new String[]{conversation_id}, json);
        try {
            return objectMapper.readTree(ret);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Deletes a conversation.
     *
     * @param conversation_id The ID of the conversation to delete.
     * @param user            The user associated with the conversation.
     * @return The HTTP response containing the result of the API request.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public void deleteConversation(String conversation_id, String user) throws DifyClientException {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("user", user);

        sendRequest(DELETE_CONVERSATION, new String[]{conversation_id}, json);
    }


    /**
     * This exception class represents a general exception that may occur while using the Dify API client.
     * It is used to handle errors related to the Dify API interactions.
     */
    public static class DifyClientException extends RuntimeException {
        public DifyClientException(String message) {
            super(message);
        }

        public DifyClientException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    /**
     * This exception class represents an exception that occurs specifically during Dify API request operations.
     * It is used to handle errors related to sending requests to the Dify API.
     */
    static class DifyRequestException extends DifyClientException {
        public DifyRequestException(String message) {
            super(message);
        }

        public DifyRequestException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class DifyRoute {
        public String method;
        public  String url;

        public DifyRoute(String method, String url) {
            this.method = method;
            this.url = url;
        }
    }

    // parse server sent event
    public void parseSSE(InputStream inputStream, OutputStream outputStream) {
        String line;
        String currentMessage = null;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
            while ((line = reader.readLine()) != null) {
                // 每次遇到空行表示当前消息结束
                if (line.isEmpty()) {
                    if (currentMessage != null) {
                        writer.write(currentMessage);
                        writer.newLine();
                        writer.flush();
                        currentMessage = null;  // Reset for the next message
                    }
                } else {  // 解析每一行
                    if (line.startsWith("data:")) {
                        currentMessage = line.substring(5);
                    }
                    // 跳过id、retry、event等
                }
            }
        } catch (IOException ignored) {
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignored) { }
            try {
                outputStream.close();
            } catch (IOException ignored) { }
        }
    }

    public static class Message {
        public String id;
        public String conversation_id;
        public String parent_message_id;
        public Object inputs;
        public String query;
        public String answer;
        public Object message_files;
        public Object feedback;
        public Object retriever_resources;
        public Date created_at;
    }

    public static class MessageList {
        public List<Message> data;
        public boolean has_more;
        public int limit;
    }

    public static class ConversationList {
        public List<Conversation> data;
        public boolean has_more;
        public int limit;
    }

    public static class Conversation {
        public String id;
        public String name;
        public Map<String, Object> inputs;
        public List<Message> messages;
        public String status;
        public String introduction;
        public Date created_at;
        public Date updated_at;
    }

    /**
     * https://github.com/ItamarBenjamin/stream-rest-template
     * */
    public static class StreamRestTemplate extends RestTemplate {
        private static final DeferredCloseClientHttpRequestInterceptor deferredCloseClientHttpRequestInterceptor =
                new DeferredCloseClientHttpRequestInterceptor();

        public StreamRestTemplate() {
            super.setInterceptors(Lists.newArrayList(deferredCloseClientHttpRequestInterceptor));
            getMessageConverters().removeIf(i -> i instanceof ResourceHttpMessageConverter);
            getMessageConverters().add(new ResourceHttpMessageConverter());
        }

        @Override
        public void setInterceptors(List<ClientHttpRequestInterceptor> interceptors) {
            super.setInterceptors(addInterceptorAtBeginning(interceptors));
        }

        private List<ClientHttpRequestInterceptor> addInterceptorAtBeginning(List<ClientHttpRequestInterceptor> interceptors) {
            boolean interceptorExists = interceptors.contains(deferredCloseClientHttpRequestInterceptor);
            if (interceptorExists && interceptors.get(0) == deferredCloseClientHttpRequestInterceptor) {
                return interceptors;
            }
            LinkedList<ClientHttpRequestInterceptor> newInterceptors = Lists.newLinkedList();
            newInterceptors.addAll(interceptors);
            if (interceptorExists) {
                newInterceptors.remove(deferredCloseClientHttpRequestInterceptor);
            }
            newInterceptors.addFirst(deferredCloseClientHttpRequestInterceptor);
            return newInterceptors;
        }

        @Override
        public  <T> ResponseExtractor<ResponseEntity<T>> responseEntityExtractor(Type responseType) {
            ResponseExtractor<ResponseEntity<T>> responseEntityResponseExtractor = super.responseEntityExtractor(responseType);
            boolean isStream = responseType == InputStreamResource.class;
            return new StreamResponseExtractor<>(isStream, responseEntityResponseExtractor);
        }

        private static class DeferredCloseClientHttpRequestInterceptor implements ClientHttpRequestInterceptor {
            @Override
            public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution) throws IOException {
                ClientHttpResponse response = execution.execute(request, body);
                return new DeferredCloseClientHttpResponse(response);
            }
        }

        @RequiredArgsConstructor
        private static class DeferredCloseClientHttpResponse implements ClientHttpResponse {
            private final ClientHttpResponse delegate;

            @Setter
            private boolean isStream = false;

            @Override
            public HttpStatus getStatusCode() throws IOException {
                return delegate.getStatusCode();
            }

            @Override
            public int getRawStatusCode() throws IOException {
                return delegate.getRawStatusCode();
            }

            @Override
            public String getStatusText() throws IOException {
                return delegate.getStatusText();
            }

            @Override
            public void close() {
                if (isStream) {
                    //do nothing, need to call close explicitly on the response
                    return;
                }
                delegate.close();
            }

            @Override
            public InputStream getBody() throws IOException {
                if (isStream) {
                    return this.new DeferredCloseInputStream(delegate.getBody());
                }
                return delegate.getBody();
            }

            @Override
            public HttpHeaders getHeaders() {
                return delegate.getHeaders();
            }


            private class DeferredCloseInputStream extends FilterInputStream {
                DeferredCloseInputStream(InputStream in) {
                    super(in);
                }

                @Override
                public void close() {
                    delegate.close();
                }
            }
        }

        @RequiredArgsConstructor
        private static class StreamResponseExtractor<T> implements ResponseExtractor<ResponseEntity<T>> {
            private final boolean isStream;
            private final ResponseExtractor<ResponseEntity<T>> delegate;

            @Override
            public ResponseEntity<T> extractData(ClientHttpResponse response) throws IOException {
                if (!(response instanceof DeferredCloseClientHttpResponse)) {
                    throw new IllegalStateException("Expected response of type DeferredCloseClientHttpResponse but got "
                            + response.getClass().getCanonicalName());
                }
                DeferredCloseClientHttpResponse deferredCloseClientHttpResponse = (DeferredCloseClientHttpResponse) response;
                deferredCloseClientHttpResponse.setStream(isStream);
                return delegate.extractData(response);
            }
        }
    }
}
