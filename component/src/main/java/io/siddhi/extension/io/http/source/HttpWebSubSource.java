/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.siddhi.extension.io.http.source;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.SystemParameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.SiddhiAppRuntimeBuilder;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.http.metrics.SourceMetrics;
import io.siddhi.extension.io.http.source.util.HttpSourceUtil;
import io.siddhi.extension.io.http.util.HttpConstants;
import io.siddhi.extension.io.http.util.HttpIoUtil;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import org.apache.log4j.Logger;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;
import org.wso2.transport.http.netty.contract.config.ListenerConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.siddhi.extension.io.http.util.HttpConstants.DEFAULT_WORKER_COUNT;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_CALLBACK;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_ID;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_ID_COLUMN_NAME;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_TOPIC;
import static io.siddhi.extension.io.http.util.HttpConstants.REQUEST_TIMESTAMP;
import static io.siddhi.extension.io.http.util.HttpConstants.SOCKET_IDEAL_TIMEOUT_VALUE;
import static io.siddhi.extension.io.http.util.HttpConstants.TOPIC_LIST;
import static io.siddhi.extension.io.http.util.HttpConstants.WEB_SUB_SUBSCRIPTION_DATA_TABLE_DEFAULT_NAME;

/**
 * {@code WebSubHub } Handle the WebSubHub Receiving task
 **/
@Extension(name = "websubhub", namespace = "source",
        description = " WebSub Hub source receive subscription requests via Http and according to the request," +
                " the subscription details will be saved to the given table and against the callback and topic name. " +
                "The subscription request  **MUST** have a Content-Type header of " +
                "**application/x-www-form-urlencoded** and following **MUST** provide as parameter body. \n " +
                "\t hub.callback \t - REQUIRED.\t The subscriber's callback URL where content distribution " +
                "notifications should be delivered. The callback URL SHOULD be an unguessable URL that is unique per " +
                "subscription.\n" +
                "\t hub.mode \t - REQUIRED.\t The literal string \"subscribe\" or \"unsubscribe\", depending on the " +
                "goal of the request.\n" +
                "\t hub.topic \t - REQUIRED.\t The topic URL that the subscriber wishes to subscribe to or " +
                "unsubscribe from.\n " +
                "\t hub.lease_seconds\t - OPTIONAL.\t Number of seconds for which the subscriber would like to have " +
                "the subscription active, given as a positive decimal integer. \n" +
                "\t hub.secret\t - OPTIONAL.\t A subscriber-provided cryptographically random unique secret string " +
                "that will be used to compute an HMAC digest for authorized content distribution. If not supplied, " +
                "the HMAC digest will not be present for content distribution requests. ",

        parameters = {
                @Parameter(name = "hub.id",
                        description = "Unique id for the WebSub Hub",
                        type = {DataType.STRING}
                ),
                @Parameter(name = "table.name",
                        description = "Table name to store the subscription details related to the hub ",
                        type = {DataType.STRING}
                ),
                @Parameter(name = "receiver.url",
                        description = "The URL on which events should be received. " +
                                "To enable SSL use `https` protocol in the url.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "`http://0.0.0.0:9763/<appNAme>/<streamName>`"
                ),
                @Parameter(name = "topic.list",
                        description = "topics allowed in the websub hub",
                        type = {DataType.STRING},
                        defaultValue = "empty"
                ),
                @Parameter(name = "basic.auth.enabled",
                        description = "This only works in VM, Docker and Kubernetes.\nWhere when enabled " +
                                "it authenticates each request using the " +
                                "`Authorization:'Basic encodeBase64(username:Password)'` header.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "false"),
                @Parameter(name = "worker.count",
                        description = "The number of active worker threads to serve the " +
                                "incoming events. By default the value is set to `1` to ensure events are processed " +
                                "in the same order they arrived. By increasing this value, " +
                                "higher performance can be achieved in the expense of loosing event ordering.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "1"),
                @Parameter(
                        name = "socket.idle.timeout",
                        description = "Idle timeout for HTTP connection in millis.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "120000"),
                @Parameter(
                        name = "ssl.verify.client",
                        description = "The type of client certificate verification. " +
                                "Supported values are `require`, `optional`.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "ssl.protocol",
                        description = "SSL/TLS protocol.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "TLS"),
                @Parameter(
                        name = "tls.store.type",
                        description = "TLS store type.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "JKS"),
                @Parameter(
                        name = "ssl.configurations",
                        description = "SSL/TSL configurations in format `\"'<key>:<value>','<key>:<value>'\"`.\n" +
                                "Some supported parameters:\n" +
                                " - SSL/TLS protocols: `'sslEnabledProtocols:TLSv1.1,TLSv1.2'`\n" +
                                " - List of ciphers: `'ciphers:TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256'`\n" +
                                " - Enable session creation: `'client.enable.session.creation:true'`\n" +
                                " - Supported server names: `'server.suported.server.names:server'`\n" +
                                " - Add HTTP SNIMatcher: `'server.supported.snimatchers:SNIMatcher'`",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "request.size.validation.configurations",
                        description = "Configurations to validate the HTTP request size.\n" +
                                "Expected format `\"'<key>:<value>','<key>:<value>'\"`.\n" +
                                "Some supported configurations :\n" +
                                " - Enable request size validation: `'request.size.validation:true'`\n" +
                                " If request size is validated\n" +
                                " - Maximum request size: `'request.size.validation.maximum.value:2048'`\n" +
                                " - Response status code when request size validation fails: " +
                                "`'request.size.validation.reject.status.code:401'`\n" +
                                " - Response message when request size validation fails: " +
                                "`'request.size.validation.reject.message:Message is bigger than the valid size'`\n" +
                                " - Response Content-Type when request size validation fails: " +
                                "`'request.size.validation.reject.message.content.type:plain/text'`",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "header.validation.configurations",
                        description = "Configurations to validate HTTP headers.\n" +
                                "Expected format `\"'<key>:<value>','<key>:<value>'\"`.\n" +
                                "Some supported configurations :\n" +
                                " - Enable header size validation: `'header.size.validation:true'`\n" +
                                " If header size is validated\n" +
                                " - Maximum length of initial line: `'header.validation.maximum.request.line:4096'`\n" +
                                " - Maximum length of all headers: `'header.validation.maximum.size:8192'`\n" +
                                " - Maximum length of the content or each chunk: " +
                                "`'header.validation.maximum.chunk.size:8192'`\n" +
                                " - Response status code when header validation fails: " +
                                "`'header.validation.reject.status.code:401'`\n" +
                                " - Response message when header validation fails: " +
                                "`'header.validation.reject.message:Message header is bigger than the valid size'`\n" +
                                " - Response Content-Type when header validation fails: " +
                                "`'header.validation.reject.message.content.type:plain/text'`",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "server.bootstrap.configurations",
                        description = "Server bootstrap configurations in " +
                                "format `\"'<key>:<value>','<key>:<value>'\"`.\n" +
                                "Some supported configurations :\n" +
                                " - Server connect timeout in millis:" +
                                " `'server.bootstrap.connect.timeout:15000'`\n" +
                                " - Server socket timeout in seconds:" +
                                " `'server.bootstrap.socket.timeout:15'`\n" +
                                " - Enable TCP no delay: `'server.bootstrap.nodelay:true'`\n" +
                                " - Enable server keep alive: `'server.bootstrap.keepalive:true'`\n" +
                                " - Send buffer size: `'server.bootstrap.sendbuffersize:1048576'`\n" +
                                " - Receive buffer size: `'server.bootstrap.recievebuffersize:1048576'`\n" +
                                " - Number of connections queued: `'server.bootstrap.socket.backlog:100'`",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "trace.log.enabled",
                        description = "Enable trace log for traffic monitoring.",
                        defaultValue = "false",
                        optional = true,
                        type = {DataType.BOOL}
                )
        },
        examples = {
                @Example(syntax = "" +
                        "@app.name('StockProcessor')\n\n" +
                        "@store(type='rdbms' , jdbc.url='jdbc:mysql://localhost:3306/production?useSSL=false', " +
                        "username='root', password='root', jdbc.driver.name='com.mysql.jdbc.Driver') \n" +
                        "@source(type='websubhub' , hub.id='anu_123',table.name='SessionTable', " +
                        "receiver.url='http://localhost:8006/productionStream',basic.auth.enabled='false', " +
                        "@map(type='keyvalue',implicit.cast.enable='true')) \n" +
                        "define stream webSubStream(callback string, lease_seconds long, secret string, " +
                        "topic string, mode string);\n",
                        description = "Above WebSubHub listening on http://localhost:8006/productionStream for the" +
                                "subscription requests.")
        },
        systemParameter = {
                @SystemParameter(
                        name = "serverBootstrapBossGroupSize",
                        description = "Number of boss threads to accept incoming connections.",
                        defaultValue = "Number of available processors",
                        possibleParameters = "Any positive integer"
                ),
                @SystemParameter(
                        name = "serverBootstrapWorkerGroupSize",
                        description = "Number of worker threads to accept the connections from boss threads and " +
                                "perform non-blocking read and write from one or more channels.",
                        defaultValue = "(Number of available processors) * 2",
                        possibleParameters = "Any positive integer"
                ),
                @SystemParameter(
                        name = "serverBootstrapClientGroupSize",
                        description = "Number of client threads to perform non-blocking read and write to " +
                                "one or more channels.",
                        defaultValue = "(Number of available processors) * 2",
                        possibleParameters = "Any positive integer"
                ),
                @SystemParameter(
                        name = "defaultHost",
                        description = "The default host of the transport.",
                        defaultValue = "0.0.0.0",
                        possibleParameters = "Any valid host"
                ),
                @SystemParameter(
                        name = "defaultScheme",
                        description = "The default protocol.",
                        defaultValue = "http",
                        possibleParameters = {"http", "https"}
                ),
                @SystemParameter(
                        name = "defaultHttpPort",
                        description = "The default HTTP port when default scheme is `http`.",
                        defaultValue = "8280",
                        possibleParameters = "Any valid port"
                ),
                @SystemParameter(
                        name = "defaultHttpsPort",
                        description = "The default HTTPS port when default scheme is `https`.",
                        defaultValue = "8243",
                        possibleParameters = "Any valid port"
                ),
                @SystemParameter(
                        name = "keyStoreLocation",
                        description = "The default keystore file path.",
                        defaultValue = "`${carbon.home}/resources/security/wso2carbon.jks`",
                        possibleParameters = "Path to `.jks` file"
                ),
                @SystemParameter(
                        name = "keyStorePassword",
                        description = "The default keystore password.",
                        defaultValue = "wso2carbon",
                        possibleParameters = "Keystore password as string"
                )
        }
)
public class HttpWebSubSource extends Source {
    private static final Logger log = Logger.getLogger(HttpWebSubSource.class);
    public String siddhiAppName;
    protected SourceMetrics metrics;
    private HttpConnectorRegistry httpConnectorRegistry;
    private SourceEventListener sourceEventListener;
    private SiddhiAppContext siddhiAppContext;
    private String listenerUrl;
    private boolean isAuth;
    private ListenerConfiguration listenerConfiguration;
    private int workerThread;
    private String[] requestedTransportPropertyNames;
    private boolean isSecured;
    private ServiceDeploymentInfo serviceDeploymentInfo;
    private Table webSubMetaTable;
    private Option httpHeaderOption;
    private String hubId;
    private String topicList;
    private List<String> topics = new ArrayList<>();

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return serviceDeploymentInfo;
    }

    @Override
    public StateFactory init(SourceEventListener sourceEventListener, OptionHolder optionHolder,
                             String[] requestedTransportPropertyNames, ConfigReader configReader,
                             SiddhiAppContext siddhiAppContext) {
        this.siddhiAppName = siddhiAppContext.getName();
        initConnectorRegistry(optionHolder, configReader);
        this.sourceEventListener = sourceEventListener;
        this.siddhiAppContext = siddhiAppContext;
        this.hubId = optionHolder.validateAndGetStaticValue(HUB_ID);
        String scheme = configReader.readConfig(HttpConstants.DEFAULT_SOURCE_SCHEME, HttpConstants
                .DEFAULT_SOURCE_SCHEME_VALUE);
        String defaultURL;
        topicList = optionHolder.validateAndGetStaticValue(TOPIC_LIST);
        topics = validateTopics(topicList);
        this.httpHeaderOption = optionHolder.getOrCreateOption(HttpConstants.HEADERS, HttpConstants.DEFAULT_HEADER);
        String webSubMetaTableName = optionHolder.validateAndGetStaticValue(
                HttpConstants.WEB_SUB_SUBSCRIPTION_DATA_TABLE_KEY, WEB_SUB_SUBSCRIPTION_DATA_TABLE_DEFAULT_NAME);
        SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder = new SiddhiAppRuntimeBuilder(this.siddhiAppContext);
        TableDefinition webSubMetaTableDefinition = generateSubscriptionTableDefinition(webSubMetaTableName,
                sourceEventListener);
        siddhiAppRuntimeBuilder.defineTable(webSubMetaTableDefinition);
        siddhiAppRuntimeBuilder.getTableDefinitionMap().put(webSubMetaTableName, webSubMetaTableDefinition);
        webSubMetaTable = siddhiAppRuntimeBuilder.getTableMap().get(webSubMetaTableName);

        int port;
        if (HttpConstants.SCHEME_HTTPS.equals(scheme)) {
            port = Integer.parseInt(configReader.readConfig(HttpConstants.HTTPS_PORT, HttpConstants.HTTPS_PORT_VALUE));
            defaultURL = HttpConstants.SCHEME_HTTPS + HttpConstants.PROTOCOL_HOST_SEPARATOR + configReader.
                    readConfig(HttpConstants.DEFAULT_HOST, HttpConstants.DEFAULT_HOST_VALUE) +
                    HttpConstants.PORT_HOST_SEPARATOR + port + HttpConstants.
                    PORT_CONTEXT_SEPARATOR + siddhiAppContext.getName()
                    + HttpConstants.PORT_CONTEXT_SEPARATOR + sourceEventListener.getStreamDefinition().getId();
        } else {
            port = Integer.parseInt(configReader.readConfig(HttpConstants.HTTP_PORT, HttpConstants.HTTP_PORT_VALUE));
            defaultURL = HttpConstants.SCHEME_HTTP + HttpConstants.PROTOCOL_HOST_SEPARATOR + configReader.
                    readConfig(HttpConstants.DEFAULT_HOST, HttpConstants.DEFAULT_HOST_VALUE) +
                    HttpConstants.PORT_HOST_SEPARATOR + port + HttpConstants.
                    PORT_CONTEXT_SEPARATOR + siddhiAppContext.getName()
                    + HttpConstants.PORT_CONTEXT_SEPARATOR + sourceEventListener.getStreamDefinition().getId();
        }
        this.listenerUrl = optionHolder.validateAndGetStaticValue(HttpConstants.RECEIVER_URL, defaultURL);
        this.isAuth = Boolean.parseBoolean(optionHolder
                .validateAndGetStaticValue(HttpConstants.IS_AUTH, HttpConstants.EMPTY_IS_AUTH)
                .toLowerCase(Locale.ENGLISH));
        this.workerThread = Integer.parseInt(optionHolder
                .validateAndGetStaticValue(HttpConstants.WORKER_COUNT, DEFAULT_WORKER_COUNT));
        this.sourceEventListener = sourceEventListener;
        this.requestedTransportPropertyNames = requestedTransportPropertyNames.clone();
        int socketIdleTimeout = Integer.parseInt(optionHolder
                .validateAndGetStaticValue(HttpConstants.SOCKET_IDEAL_TIMEOUT, SOCKET_IDEAL_TIMEOUT_VALUE));
        String verifyClient = optionHolder
                .validateAndGetStaticValue(HttpConstants.SSL_VERIFY_CLIENT, HttpConstants.EMPTY_STRING);
        String sslProtocol = optionHolder
                .validateAndGetStaticValue(HttpConstants.SSL_PROTOCOL, HttpConstants.EMPTY_STRING);
        String tlsStoreType = optionHolder
                .validateAndGetStaticValue(HttpConstants.TLS_STORE_TYPE, HttpConstants.EMPTY_STRING);
        String requestSizeValidationConfigList = optionHolder
                .validateAndGetStaticValue(HttpConstants.REQUEST_SIZE_VALIDATION_CONFIGS, HttpConstants.EMPTY_STRING);
        String sslConfigs = optionHolder
                .validateAndGetStaticValue(HttpConstants.SSS_CONFIGS, HttpConstants.EMPTY_STRING);
        if (sslConfigs.equalsIgnoreCase(HttpConstants.EMPTY_STRING)) {
            sslConfigs = optionHolder
                    .validateAndGetStaticValue(HttpConstants.SOURCE_PARAMETERS, HttpConstants.EMPTY_STRING);
        }
        String traceLog = optionHolder.validateAndGetStaticValue(HttpConstants.TRACE_LOG_ENABLED, configReader
                .readConfig(HttpConstants.DEFAULT_TRACE_LOG_ENABLED, HttpConstants.EMPTY_STRING));
        this.listenerConfiguration = HttpSourceUtil.getListenerConfiguration(this.listenerUrl, configReader);
        if (socketIdleTimeout != -1) {
            this.listenerConfiguration.setSocketIdleTimeout(socketIdleTimeout);
        }
        if (!HttpConstants.EMPTY_STRING.equals(verifyClient)) {
            this.listenerConfiguration.setVerifyClient(verifyClient);
        }
        if (!HttpConstants.EMPTY_STRING.equals(sslProtocol)) {
            this.listenerConfiguration.setSSLProtocol(sslProtocol);
        }
        if (!HttpConstants.EMPTY_STRING.equals(tlsStoreType)) {
            this.listenerConfiguration.setTLSStoreType(tlsStoreType);
        }
        if (!HttpConstants.EMPTY_STRING.equals(traceLog)) {
            this.listenerConfiguration.setHttpTraceLogEnabled(Boolean.parseBoolean(traceLog));
        }
        if (!HttpConstants.EMPTY_STRING.equals(requestSizeValidationConfigList)) {
            this.listenerConfiguration.setMsgSizeValidationConfig(HttpConnectorRegistry.getInstance()
                    .populateRequestSizeValidationConfiguration());
        }
        isSecured = (listenerConfiguration.getScheme().equalsIgnoreCase(HttpConstants.SCHEME_HTTPS));
        port = listenerConfiguration.getPort();
        listenerConfiguration.setParameters(HttpIoUtil.populateParameters(sslConfigs));
        serviceDeploymentInfo = new ServiceDeploymentInfo(port, isSecured);
        initMetrics(siddhiAppContext.getName());
        return null;
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{Map.class};
    }

    @Override
    public void connect(ConnectionCallback connectionCallback, State state) throws ConnectionUnavailableException {
        this.httpConnectorRegistry.createHttpServerConnector(listenerConfiguration, metrics);
        this.httpConnectorRegistry.registerSourceListener(sourceEventListener, listenerUrl, workerThread, isAuth,
                requestedTransportPropertyNames, siddhiAppName, metrics, webSubMetaTable, hubId, siddhiAppContext,
                topics);
        webSubMetaTable.connectWithRetry();
    }

    protected void initConnectorRegistry(OptionHolder optionHolder, ConfigReader configReader) {
        String requestSizeValidationConfigList = optionHolder
                .validateAndGetStaticValue(HttpConstants.REQUEST_SIZE_VALIDATION_CONFIGS, HttpConstants.EMPTY_STRING);
        String serverBootstrapPropertiesList = optionHolder
                .validateAndGetStaticValue(HttpConstants.SERVER_BOOTSTRAP_CONFIGS, HttpConstants.EMPTY_STRING);
        this.httpConnectorRegistry = HttpConnectorRegistry.getInstance();
        this.httpConnectorRegistry.initBootstrapConfigIfFirst(configReader);
        this.httpConnectorRegistry.setTransportConfig(serverBootstrapPropertiesList, requestSizeValidationConfigList);
    }

    @Override
    public void disconnect() {
        this.httpConnectorRegistry.unregisterSourceListener(this.listenerUrl, siddhiAppName, metrics);
        this.httpConnectorRegistry.unregisterServerConnector(this.listenerUrl);
    }

    @Override
    public void destroy() {
        this.httpConnectorRegistry.clearBootstrapConfigIfLast();
    }

    @Override
    public void pause() {
        HttpSourceListener httpSourceListener = this.httpConnectorRegistry.getSourceListenersMap().get(HttpSourceUtil
                .getSourceListenerKey(listenerUrl, metrics));
        if ((httpSourceListener != null) && (httpSourceListener.isRunning())) {
            httpSourceListener.pause();
        }
    }

    @Override
    public void resume() {
        HttpSourceListener httpSourceListener = this.httpConnectorRegistry.getSourceListenersMap()
                .get(HttpSourceUtil.getSourceListenerKey(listenerUrl, metrics));
        if ((httpSourceListener != null) && (httpSourceListener.isPaused())) {
            httpSourceListener.resume();
        }
    }

    /**
     * Initialize metrics.
     */
    protected void initMetrics(String appName) {
        if (MetricsDataHolder.getInstance().getMetricService() != null
                && MetricsDataHolder.getInstance().getMetricManagementService().isEnabled()) {
            try {
                if (MetricsDataHolder.getInstance().getMetricManagementService()
                        .isReporterRunning(HttpConstants.PROMETHEUS_REPORTER_NAME)) {
                    metrics = new SourceMetrics(appName, sourceEventListener.getStreamDefinition().getId(),
                            listenerUrl);
                }
            } catch (IllegalArgumentException e) {
                log.debug("Prometheus reporter is not running. Hence http source metrics will not be initialized for "
                        + appName);
            }
        }
    }

    private TableDefinition generateSubscriptionTableDefinition(String tableName,
                                                                SourceEventListener sourceEventListener) {
        List<Annotation> annotations = new ArrayList<>();
        List<Annotation> annotationsInSourceDefinition = sourceEventListener.getStreamDefinition().getAnnotations();
        for (Annotation annotation : annotationsInSourceDefinition) {
            if (annotation.getName().equalsIgnoreCase(SiddhiConstants.ANNOTATION_STORE)) {
                annotations.add(annotation);
            }
        }
        Annotation primaryKeyAnnotation = new Annotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY);
        Annotation indexKeyAnnotation = new Annotation(SiddhiConstants.ANNOTATION_INDEX);
        TableDefinition tableDefinition = TableDefinition.id(tableName);
        List<Attribute> attributeList = sourceEventListener.getStreamDefinition().getAttributeList();
        for (Attribute attribute : attributeList) {
            tableDefinition.attribute(attribute.getName(), attribute.getType());
        }
        primaryKeyAnnotation.element(HUB_CALLBACK);
        primaryKeyAnnotation.element(HUB_TOPIC);
        indexKeyAnnotation.element(HUB_ID_COLUMN_NAME);
        annotations.add(indexKeyAnnotation);
        annotations.add(primaryKeyAnnotation);
        tableDefinition.attribute(HUB_ID_COLUMN_NAME, Attribute.Type.STRING);
        for (Annotation annotation : annotations) {
            tableDefinition.annotation(annotation);
        }
        tableDefinition.attribute(REQUEST_TIMESTAMP, Attribute.Type.LONG);
        return tableDefinition;
    }

    private List<String> validateTopics(String topicList) {
        String[] topics = Arrays.stream(topicList.split(",")).map(String::trim).toArray(String[]::new);
        return new ArrayList<>(Arrays.asList(topics));
    }
}
