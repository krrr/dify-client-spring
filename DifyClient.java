package com.krrr.dify.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.val;
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
import java.util.stream.Collectors;

/**
 * This class serves as a client for interacting with the Dify API.
 * It provides methods for sending various types of requests to the API.
 */
public class DifyClient {
    public static final String OFFICIAL_API_URL = "https://api.dify.ai/v1";

    // Constants representing different API routes
    // chat
    private static final DifyRoute APP_PARAMETERS = new DifyRoute("GET", "/parameters", DifyRoute.TYPE_CHAT);
    private static final DifyRoute APP_INFO = new DifyRoute("GET", "/info", DifyRoute.TYPE_CHAT);
    private static final DifyRoute FEEDBACK = new DifyRoute("POST", "/messages/%s/feedbacks", DifyRoute.TYPE_CHAT);
    private static final DifyRoute STOP_GENERATION = new DifyRoute("POST", "/chat-messages/%s/stop", DifyRoute.TYPE_CHAT);
    private static final DifyRoute CREATE_CHAT_MESSAGE = new DifyRoute("POST", "/chat-messages", DifyRoute.TYPE_CHAT);
    private static final DifyRoute GET_CONVERSATION_MESSAGES = new DifyRoute("GET", "/messages?%s", DifyRoute.TYPE_CHAT);
    private static final DifyRoute GET_CONVERSATIONS = new DifyRoute("GET", "/conversations?%s", DifyRoute.TYPE_CHAT);
    private static final DifyRoute RENAME_CONVERSATION = new DifyRoute("POST", "/conversations/%s/name", DifyRoute.TYPE_CHAT);
    private static final DifyRoute DELETE_CONVERSATION = new DifyRoute("DELETE", "/conversations/%s", DifyRoute.TYPE_CHAT);
    // knowledge base
    private static final DifyRoute GET_DATASETS = new DifyRoute("GET", "/datasets", DifyRoute.TYPE_KNOWLEDGE_BASE);
    private static final DifyRoute CREATE_DOC_TXT = new DifyRoute("POST", "/datasets/%s/document/create-by-text", DifyRoute.TYPE_KNOWLEDGE_BASE);
    private static final DifyRoute UPDATE_DOC_TXT = new DifyRoute("POST", "/datasets/%s/documents/%s/update-by-text", DifyRoute.TYPE_KNOWLEDGE_BASE);
    private static final DifyRoute GET_DOCUMENTS = new DifyRoute("POST", "/datasets/%s/retrieve", DifyRoute.TYPE_KNOWLEDGE_BASE);
    private static final DifyRoute DELETE_DOCUMENT = new DifyRoute("DELETE", "/datasets/%s/documents/%s", DifyRoute.TYPE_KNOWLEDGE_BASE);

    private String chatApiKey;
    private String knowledgeBaseApiKey;
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
     * @param chatApiKey            The API key to use for chat.
     * @param knowledgeBaseApiKey   The API key to use for knowledge base. Can be null.
     */
    public DifyClient(String chatApiKey, String knowledgeBaseApiKey) {
        this(chatApiKey, knowledgeBaseApiKey, OFFICIAL_API_URL);
    }

    /**
     * Constructs a new DifyClient with the provided API key and base URL.
     *
     * @param chatApiKey   The API key to use for authentication.
     * @param knowledgeBaseApiKey   The API key to use for knowledge base. Can be null.
     * @param baseUrl  The base URL of the Dify API.
     */
    public DifyClient(String chatApiKey, String knowledgeBaseApiKey, String baseUrl) {
        this.chatApiKey = chatApiKey;
        this.knowledgeBaseApiKey = knowledgeBaseApiKey;
        this.baseUrl = baseUrl;
        RestTemplateBuilder builder = new RestTemplateBuilder()
                .setConnectTimeout(Duration.ofSeconds(connectTimeout)).setReadTimeout(Duration.ofSeconds(readTimeout));
        restTemplate = builder.build();
        streamRestTemplate = builder.build(StreamRestTemplate.class);
        // init json parser
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }

    /**
     * Updates the API key used for chat.
     */
    public void updateChatApiKey(String chatApiKey) {
        this.chatApiKey = chatApiKey;
    }

    /**
     * Updates the API key used for knowledge base.
     */
    public void updateKnowledgeBaseApiKey(String knowledgeBaseApiKey) {
        this.knowledgeBaseApiKey = knowledgeBaseApiKey;
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
    private String sendRequest(DifyRoute route, String[] formatArgs, JsonNode body) throws DifyClientException {
        try {
            String formattedURL = (formatArgs != null && formatArgs.length > 0)
                    ? String.format(route.url, (Object[]) formatArgs)
                    : route.url;

            val headers = new HttpHeaders();
            headers.set(HttpHeaders.AUTHORIZATION, "Bearer " + (route.type == DifyRoute.TYPE_CHAT ? chatApiKey : knowledgeBaseApiKey));
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> entity = body != null
                    ? new HttpEntity<>(objectMapper.writeValueAsString(body), headers)
                    : new HttpEntity<>(headers);

            val resp = restTemplate.exchange(baseUrl + formattedURL, HttpMethod.valueOf(route.method), entity, String.class);

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
     * @return Piped stream
     * @throws DifyClientException If an error occurs while sending the request.
     */
     private PipedInputStream sendRequestStream(DifyRoute route, String[] formatArgs, JsonNode body) throws DifyClientException {
        try {
            String formattedURL = (formatArgs != null && formatArgs.length > 0)
                    ? String.format(route.url, (Object[]) formatArgs)
                    : route.url;

            HttpHeaders headers = new HttpHeaders();
            headers.set(HttpHeaders.AUTHORIZATION, "Bearer " + (route.type == DifyRoute.TYPE_CHAT ? chatApiKey : knowledgeBaseApiKey));
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

    ///////////////// chat api /////////////////

    /**
     * Stops the generation of a chat message.
     *
     * @param taskId Task ID, which can be obtained from the streaming returned Chunk.
     * @param user   The user associated with the chat message.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public void stopGeneration(String taskId, String user) throws DifyClientException {
        val json = objectMapper.createObjectNode();
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
     * Sends a message feedback.
     *
     * @param messageId The ID of the message to provide feedback for.
     * @param rating    The feedback rating.
     * @param user      The user providing the feedback.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public void messageFeedback(String messageId, String rating, String user, String content) throws DifyClientException {
        val json = objectMapper.createObjectNode();
        json.put("rating", rating);
        json.put("user", user);
        json.put("content", content);

        sendRequest(FEEDBACK, new String[]{messageId}, json);
    }

    /**
     * Retrieves application basic info.
     *
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public Map<String, Object> getApplicationInfo() throws DifyClientException {
        String ret = sendRequest(APP_INFO, null, null);
        try {
            return objectMapper.readValue(ret, new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Retrieves application parameters.
     *
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public Map<String, Object> getApplicationParameters() throws DifyClientException {
        String ret = sendRequest(APP_PARAMETERS, null, null);
        try {
            return objectMapper.readValue(ret, new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Creates a new chat message.
     *
     * @param inputs         The chat message inputs.
     * @param query          The query associated with the chat message.
     * @param user           The user associated with the chat message.
     * @param conversationId The ID of the conversation, if applicable.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public Message createChatMessage(JsonNode inputs, String query, String user, String conversationId) throws DifyClientException {
        val json = objectMapper.createObjectNode();
        json.put("inputs", inputs);
        json.put("query", query);
        json.put("user", user);
        json.put("response_mode", "blocking");
        json.put("auto_generate_name", autoGenerateName);
        if (conversationId != null && !conversationId.isEmpty()) {
            json.put("conversation_id", conversationId);
        }

        String ret = sendRequest(CREATE_CHAT_MESSAGE, null, json);
        try {
            return objectMapper.readValue(ret, Message.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Creates a new chat message (SSE Stream version).
     */
    public PipedInputStream createChatMessageStream(JsonNode inputs, String query, String user, String conversationId) throws DifyClientException {
        val json = objectMapper.createObjectNode();
        json.put("inputs", inputs);
        json.put("query", query);
        json.put("user", user);
        json.put("response_mode", "streaming");
        json.put("auto_generate_name", autoGenerateName);
        if (conversationId != null && !conversationId.isEmpty()) {
            json.put("conversation_id", conversationId);
        }

        return sendRequestStream(CREATE_CHAT_MESSAGE, null, json);
    }

    /**
     * Retrieves conversation messages.
     *
     * @param user           The user associated with the conversation.
     * @param conversationId The ID of the conversation.
     * @param firstId       The ID of the first message to start fetching from.
     * @param limit          The maximum number of messages to retrieve.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public PagedResult<Message> getConversationMessages(String user, String conversationId, String firstId, Integer limit) throws DifyClientException {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("user", user);

        if (conversationId != null) {
            queryParams.put("conversation_id", conversationId);
        }
        if (firstId != null) {
            queryParams.put("first_id", firstId);
        }
        if (limit != null) {
            queryParams.put("limit", String.valueOf(limit));
        }
        String formattedQueryParams = generateQueryParams(queryParams);

        String json = sendRequest(GET_CONVERSATION_MESSAGES, new String[] {formattedQueryParams}, null);
        try {
            return objectMapper.readValue(json, new TypeReference<PagedResult<Message>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Retrieves conversations.
     *
     * @param user       The user associated with the conversations.
     * @param lastId    The ID of the last record on the current page, default is null.
     * @param limit      The maximum number of conversations to retrieve.
     * @param sortBy     The sorting criteria for the conversations.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public PagedResult<Conversation> getConversations(String user, String lastId, Integer limit, String sortBy) throws DifyClientException {
        Map<String, String> queryParams = new HashMap<>();
        queryParams.put("user", user);
        if (lastId != null && !lastId.isEmpty()) {
            queryParams.put("last_id", lastId);
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
            return objectMapper.readValue(json, new TypeReference<PagedResult<Conversation>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Renames a conversation.
     *
     * @param conversationId The ID of the conversation to rename.
     * @param name            The new name for the conversation.
     * @param user            The user associated with the conversation.
     * @return The HTTP response containing the result of the API request.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public Conversation renameConversation(String conversationId, String name, String user) throws DifyClientException {
        val json = objectMapper.createObjectNode();
        json.put("name", name);
        json.put("user", user);

        val ret = sendRequest(RENAME_CONVERSATION, new String[]{conversationId}, json);
        try {
            return objectMapper.readValue(ret, Conversation.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Deletes a conversation.
     *
     * @param conversationId The ID of the conversation to delete.
     * @param user            The user associated with the conversation.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public void deleteConversation(String conversationId, String user) throws DifyClientException {
        val json = objectMapper.createObjectNode();
        json.put("user", user);

        sendRequest(DELETE_CONVERSATION, new String[]{conversationId}, json);
    }

    ///////////////// knowledge base api /////////////////

    /**
     * Get datasets.
     */
    public PagedResult<KnowledgeBase> getDatasets(int page, Integer limit) {
        val queryParams = new HashMap<String, String>();
        queryParams.put("page", String.valueOf(page));
        if (limit != null) {
            queryParams.put("limit", String.valueOf(limit));
        }
        val formattedQueryParams = generateQueryParams(queryParams);
        val ret = sendRequest(GET_DATASETS, new String[] {formattedQueryParams}, null);
        try {
            return objectMapper.readValue(ret, new TypeReference<PagedResult<KnowledgeBase>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Create document by text.
     * @param datasetId    The ID of knowledge base.
     */
    public DocResult createDocByTxt(String datasetId, DocCreateParam param) {
        val ret = sendRequest(CREATE_DOC_TXT, new String[]{datasetId}, objectMapper.valueToTree(param));
        try {
            return objectMapper.readValue(ret, DocResult.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Update document by text.
     * @param datasetId    The ID of knowledge base.
     * @param documentId   The ID of document.
     */
    public DocResult updateDocByTxt(String datasetId, String documentId, DocUpdateParam param) {
        val ret = sendRequest(UPDATE_DOC_TXT, new String[]{datasetId, documentId}, objectMapper.valueToTree(param));
        try {
            return objectMapper.readValue(ret, DocResult.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }


    /**
     * Deletes a conversation.
     *
     * @param datasetId    The ID of knowledge base.
     * @param documentId   The ID of document.
     * @throws DifyClientException If an error occurs while sending the request.
     */
    public void deleteDoc(String datasetId, String documentId) throws DifyClientException {
        sendRequest(DELETE_DOCUMENT, new String[]{datasetId, documentId}, null);
    }

    /**
     * Get documents.
     */
    public List<Document> getDocuments(String datasetId, String keyword) throws DifyClientException {
        Map<String, String> queryParams = new HashMap<>();
        if (keyword != null && !keyword.isEmpty()) {
            queryParams.put("keyword", keyword);
        }
        String formattedQueryParams = generateQueryParams(queryParams);
        String json = sendRequest(GET_DOCUMENTS, new String[]{datasetId, formattedQueryParams}, null);
        try {
            return objectMapper.readValue(json, new TypeReference<List<Document>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    /**
     * retrieve knowledge
     */
    public RetrieveResult retrieveKnowledge(String datasetId, RetrieveKnowledgeParam param) throws DifyClientException {
        String json = sendRequest(GET_DOCUMENTS, new String[] {datasetId}, objectMapper.valueToTree(param));
        try {
            return objectMapper.readValue(json, RetrieveResult.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

    ///////////////// models /////////////////

    // chat

    public static class Message {
        public String id;
        public String conversationId;
        public String parentMessageId;
        public Map<String, Object> inputs;
        public String query;
        public String answer;
        public Object messageFiles;
        public Object feedback;
        public Object retrieverResources;
        public Date createdAt;
    }

    public static class PagedResult<T> {
        public List<T> data;
        public boolean hasMore;
        public int limit;
    }

    public static class Conversation {
        public String id;
        public String name;
        public Map<String, Object> inputs;
        public List<Message> messages;
        public String status;
        public String introduction;
        public Date createdAt;
        public Date updatedAt;
    }

    public static class Document {
        public String id;
        public int position;
        public String dataSourceType;
        public Object dataSourceInfo;
        public String datasetProcessRuleId;
        public String name;
        public String createdFrom;
        public String createdBy;
        public Date createdAt;
        public int tokens;
        public String indexingStatus;
        public Object error;
        public boolean enabled;
        public Object disabledAt;
        public Object disabledBy;
        public boolean archived;
    }

    // knowledge base

    public static class KnowledgeBase {
        public String id;
        public String name;
        public String description;
        public String permission;
        public String dataSourceType;
        public String indexingTechnique;
        public int appCount;
        public int documentCount;
        public int wordCount;
        public String createdBy;
        public Date createdAt;
        public String updatedBy;
        public Date updatedAt;
    }

    public static class DocCreateParam {
        public String name;  // 文档名称
        public String text;  // 文档内容
        public String docType;  // 文档类型（选填）
        public Map<String, Object> docMetadata;  // 文档元数据（如提供文档类型则必填）
        public String indexingTechnique;  // 索引方式：高质量/high_quality 或 经济/economy
        public String docForm = "text_model";  // 索引内容的形式：text_model、hierarchical_model 或 qa_model
        public String docLanguage = "English";  // 在 Q&A 模式下，指定文档的语言，例如：English、Chinese
        public DocProcessRule processRule = new DocProcessRule();  // 处理规则
        public RetrievalModel retrievalModel;  // 检索模式（首次上传时需要提供）
        public String embeddingModel;  // Embedding 模型名称
        public String embeddingModelProvider;  // Embedding 模型供应商


        public static class RetrievalModel {
            public String searchMethod;  // 检索方法：hybrid_search、semantic_search 或 full_text_search
            public boolean rerankingEnable;  // 是否开启 rerank
            public RerankingModel rerankingModel;  // Rerank 模型配置
            public int topK;  // 召回条数
            public boolean scoreThresholdEnabled;  // 是否开启召回分数限制
            public float scoreThreshold;  // 召回分数限制

            public static class RerankingModel {
                public String rerankingProviderName;  // Rerank 模型的提供商
                public String rerankingModelName;  // Rerank 模型的名称
            }
        }
    }

    public static class DocResult {
        public Document document;
        public String batch;
    }

    public static class RetrieveResult {
        public QueryWrapper query;
        public List<Record> records;

        public static class QueryWrapper {
            public String content;
        }

        public static class Record {
            public Segment segment;
            public double score;
            public Object tsnePosition;

            public static class Segment {
                public String id;
                public int position;
                public String documentId;
                public String content;
                public Object answer;
                public int wordCount;
                public int tokens;
                public List<String> keywords;
                public String indexNodeId;
                public String indexNodeHash;
                public int hitCount;
                public boolean enabled;
                public Object disabledAt;
                public Object disabledBy;
                public String status;
                public String createdBy;
                public long createdAt;
                public long indexingAt;
                public long completedAt;
                public Object error;
                public Object stoppedAt;
                public Document document;

                public static class Document {
                    public String id;
                    public String dataSourceType;
                    public String name;
                    public Object docType;
                }
            }
        }
    }

    public static class RetrieveKnowledgeParam {
        public String query;  // 检索关键词
        public RetrievalModel retrievalModel;  // 检索参数（选填）

        public static class RetrievalModel {
            public String searchMethod;  // 检索方法：keyword_search, semantic_search, full_text_search, hybrid_search
            public Boolean rerankingEnable;  // 是否启用 Reranking
            public RerankingMode rerankingMode;  // Rerank 模型配置
            public Float weights;  // 混合检索模式下语意检索的权重设置
            public Integer topK;  // 返回结果数量
            public Boolean scoreThresholdEnabled;  // 是否开启 score 阈值
            public Float scoreThreshold;  // Score 阈值

            public static class RerankingMode {
                public String rerankingProviderName;  // Rerank 模型提供商
                public String rerankingModelName;  // Rerank 模型名称
            }
        }
    }

    public static class DocUpdateParam {
        public String name;  // 文档名称
        public String text;  // 文档内容
        public DocProcessRule processRule;  // 处理规则
    }

    public static class DocProcessRule {
        public String mode = "automatic";  // 清洗、分段模式：automatic 自动 / custom 自定义
        public Rules rules;  // 自定义规则（自动模式下，该字段为空）

        public static class Rules {
            public List<PreProcessingRule> preProcessingRules;  // 预处理规则
            public Segmentation segmentation;  // 分段规则
            public String parentMode;  // 父分段的召回模式：full-doc 全文召回 / paragraph 段落召回
            public SubchunkSegmentation subchunkSegmentation;  // 子分段规则

            public static class PreProcessingRule {
                public String id;  // 预处理规则的唯一标识符
                public boolean enabled;  // 是否选中该规则
            }

            public static class Segmentation {
                public String separator;  // 自定义分段标识符，默认为 \n
                public int maxTokens;  // 最大长度（token），默认为 1000
            }

            public static class SubchunkSegmentation {
                public String separator;  // 分段标识符，默认为 ***
                public int maxTokens;  // 最大长度（token），需要校验小于父级的长度
                public int chunkOverlap;  // 分段重叠部分（选填）
            }
        }
    }


    ///////////////// utils /////////////////

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

    @AllArgsConstructor
    private static class DifyRoute {
        public String method;
        public String url;
        public int type;

        public static final int TYPE_CHAT = 0;
        public static final int TYPE_KNOWLEDGE_BASE = 1;
    }

    // parse server sent event
    private void parseSSE(InputStream inputStream, OutputStream outputStream) {
        String line;
        String currentMessage = null;

        try (val reader = new BufferedReader(new InputStreamReader(inputStream));
             val writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
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

    /**
     * Generates query parameters in the form of key-value pairs joined by "&".
     *
     * @param params The map of query parameter key-value pairs.
     * @return A string representation of the generated query parameters.
     */
    private String generateQueryParams(Map<String, String> params) {
        return params.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("&"));
    }

    /**
     * https://github.com/ItamarBenjamin/stream-rest-template
     * */
    private static class StreamRestTemplate extends RestTemplate {
        private static final DeferredCloseClientHttpRequestInterceptor deferredCloseClientHttpRequestInterceptor =
                new DeferredCloseClientHttpRequestInterceptor();

        public StreamRestTemplate() {
            super.setInterceptors(Collections.singletonList(deferredCloseClientHttpRequestInterceptor));
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
            val newInterceptors = new LinkedList<>(interceptors);
            if (interceptorExists) {
                newInterceptors.remove(deferredCloseClientHttpRequestInterceptor);
            }
            newInterceptors.addFirst(deferredCloseClientHttpRequestInterceptor);
            return newInterceptors;
        }

        @Override
        public <T> ResponseExtractor<ResponseEntity<T>> responseEntityExtractor(Type responseType) {
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

