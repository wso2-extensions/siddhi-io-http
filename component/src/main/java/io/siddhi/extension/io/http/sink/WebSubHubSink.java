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

package io.siddhi.extension.io.http.sink;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.SystemParameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.aggregation.AggregationRuntime;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.query.OnDemandQueryRuntime;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.SiddhiAppRuntimeBuilder;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.parser.OnDemandQueryParser;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.core.window.Window;
import io.siddhi.extension.io.http.metrics.EndpointStatus;
import io.siddhi.extension.io.http.metrics.SinkMetrics;
import io.siddhi.extension.io.http.sink.util.HttpSinkUtil;
import io.siddhi.extension.io.http.util.HttpConstants;
import io.siddhi.extension.io.http.util.HttpIoUtil;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.OnDemandQuery;
import io.siddhi.query.api.execution.query.input.store.InputStore;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.Compare;
import io.siddhi.query.api.expression.constant.StringConstant;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;
import org.wso2.carbon.messaging.Header;
import org.wso2.carbon.si.metrics.core.internal.MetricsDataHolder;
import org.wso2.transport.http.netty.contract.Constants;
import org.wso2.transport.http.netty.contract.HttpConnectorListener;
import org.wso2.transport.http.netty.contract.HttpResponseFuture;
import org.wso2.transport.http.netty.contract.config.ChunkConfig;
import org.wso2.transport.http.netty.contract.config.ProxyServerConfiguration;
import org.wso2.transport.http.netty.contract.config.SenderConfiguration;
import org.wso2.transport.http.netty.contractimpl.DefaultHttpWsConnectorFactory;
import org.wso2.transport.http.netty.contractimpl.sender.channel.pool.PoolConfiguration;
import org.wso2.transport.http.netty.message.HttpCarbonMessage;
import org.wso2.transport.http.netty.message.HttpMessageDataStreamer;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.siddhi.extension.io.http.sink.util.HttpSinkUtil.createConnectorFactory;
import static io.siddhi.extension.io.http.sink.util.HttpSinkUtil.createPoolConfigurations;
import static io.siddhi.extension.io.http.sink.util.HttpSinkUtil.createProxyServerConfiguration;
import static io.siddhi.extension.io.http.util.HttpConstants.EMPTY_STRING;
import static io.siddhi.extension.io.http.util.HttpConstants.FALSE;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_CALLBACK;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_ID;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_ID_COLUMN_NAME;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_LEASE_SECONDS;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_MODE_COLUMN_NAME;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_SECRET;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_TOPIC;
import static io.siddhi.extension.io.http.util.HttpConstants.PORT_HOST_SEPARATOR;
import static io.siddhi.extension.io.http.util.HttpConstants.REQUEST_TIMESTAMP;
import static io.siddhi.extension.io.http.util.HttpConstants.SHA256_HASHING;
import static io.siddhi.extension.io.http.util.HttpConstants.SOCKET_IDEAL_TIMEOUT_VALUE;
import static io.siddhi.extension.io.http.util.HttpConstants.TRUE;
import static io.siddhi.extension.io.http.util.HttpConstants.WEB_SUB_SUBSCRIPTION_DATA_TABLE_KEY;
import static io.siddhi.extension.io.http.util.HttpConstants.WEB_SUB_SUBSCRIPTION_MAP_UPDATE_TIMESTAMP;
import static io.siddhi.extension.io.http.util.HttpConstants.X_HUB_SIGNATURE;
import static org.wso2.carbon.analytics.idp.client.external.ExternalIdPClientConstants.REQUEST_URL;

/**
 * {@code WebSubHubEventPublisher } Handle the WebSubHub Publishing task
 */
@Extension(name = "websubhub", namespace = "sink",
        description = "" +
                "WebSubHubEventPublisher publishes messages via HTTP/HTTP according to the provided URL when " +
                "subscribe to the WebSub hub. The table.name, hub.id are mandatory when defining the websubhub source ",
        parameters = {
                @Parameter(
                        name = "hub.id",
                        description = "Id of the hub that the messages needed to process ",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "table.name",
                        description = "Name of the table which subscription data holds related to the hub  ",
                        type = {DataType.STRING}),
                @Parameter(
                        name = "https.truststore.file",
                        description = "The file path of the client truststore when sending messages through `https`" +
                                " protocol.",
                        type = {DataType.STRING},
                        optional = true, defaultValue = "`${carbon.home}/resources/security/client-truststore.jks`"),
                @Parameter(
                        name = "https.truststore.password",
                        description = "The password for the client-truststore.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "wso2carbon"),
                @Parameter(
                        name = "consumer.key",
                        description = "Consumer key used for calling endpoints protected by OAuth 2.0",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "consumer.secret",
                        description = "Consumer secret used for calling endpoints protected by OAuth 2.0",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "token.url",
                        description = "Token URL to generate a new access tokens " +
                                "when calling endpoints protected by OAuth 2.0",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "refresh.token",
                        description = "Refresh token used for generating new access tokens " +
                                "when calling endpoints protected by OAuth 2.0",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "headers",
                        description = "HTTP request headers in format `\"'<key>:<value>','<key>:<value>'\"`.\n" +
                                "When `Content-Type` header is not provided the system derives the " +
                                "Content-Type based on the provided sink mapper as following: \n" +
                                " - `@map(type='xml')`: `application/xml`\n" +
                                " - `@map(type='json')`: `application/json`\n" +
                                " - `@map(type='text')`: `plain/text`\n" +
                                " - `@map(type='keyvalue')`: `application/x-www-form-urlencoded`\n" +
                                " - For all other cases system defaults to `plain/text`\n" +
                                "Also the `Content-Length` header need not to be provided, as the system " +
                                "automatically defines it by calculating the size of the payload.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "Content-Type and Content-Length headers"),
                @Parameter(
                        name = "method",
                        description = "The HTTP method used for calling the endpoint.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "POST"),
                @Parameter(
                        name = "socket.idle.timeout",
                        description = "Socket timeout in millis.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "6000"),
                @Parameter(
                        name = "chunk.disabled",
                        description = "Disable chunked transfer encoding.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"),
                @Parameter(
                        name = "ssl.protocol",
                        description = "SSL/TLS protocol.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "TLS"),
                @Parameter(
                        name = "ssl.verification.disabled",
                        description = "Disable SSL verification.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"),
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
                        name = "proxy.host",
                        description = "Proxy server host",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "proxy.port",
                        description = "Proxy server port",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "proxy.username",
                        description = "Proxy server username",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "proxy.password",
                        description = "Proxy server password",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "client.bootstrap.configurations",
                        description = "Client bootstrap configurations in format " +
                                "`\"'<key>:<value>','<key>:<value>'\"`.\n" +
                                "Some supported configurations :\n" +
                                " - Client connect timeout in millis: `'client.bootstrap.connect.timeout:15000'`\n" +
                                " - Client socket timeout in seconds: `'client.bootstrap.socket.timeout:15'`\n" +
                                " - Client socket reuse: `'client.bootstrap.socket.reuse:true'`\n" +
                                " - Enable TCP no delay: `'client.bootstrap.nodelay:true'`\n" +
                                " - Enable client keep alive: `'client.bootstrap.keepalive:true'`\n" +
                                " - Send buffer size: `'client.bootstrap.sendbuffersize:1048576'`\n" +
                                " - Receive buffer size: `'client.bootstrap.recievebuffersize:1048576'`",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "-"),
                @Parameter(
                        name = "max.pool.active.connections",
                        description = "Maximum possible number of active connection per client pool.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "-1"),
                @Parameter(
                        name = "min.pool.idle.connections",
                        description = "Minimum number of idle connections that can exist per client pool.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "0"),
                @Parameter(
                        name = "max.pool.idle.connections",
                        description = "Maximum number of idle connections that can exist per client pool.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "100"),
                @Parameter(
                        name = "min.evictable.idle.time",
                        description = "Minimum time (in millis) a connection may sit idle in the " +
                                "client pool before it become eligible for eviction.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "300000"),
                @Parameter(
                        name = "time.between.eviction.runs",
                        description = "Time between two eviction operations (in millis) on the client pool.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "30000"),
                @Parameter(
                        name = "max.wait.time",
                        description = "The maximum time (in millis) the pool will wait (when there are no " +
                                "available connections) for a connection to be returned to the pool.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "60000"),
                @Parameter(
                        name = "test.on.borrow",
                        description = "Enable connections to be validated " +
                                "before being borrowed from the client pool.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"),
                @Parameter(
                        name = "test.while.idle",
                        description = "Enable connections to be validated during the eviction operation (if any).",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"),
                @Parameter(
                        name = "exhausted.action",
                        description = "Action that should be taken when the maximum number of active connections " +
                                "are being used. This action should be indicated as an int and possible " +
                                "action values are following.\n" +
                                "0 - Fail the request.\n" +
                                "1 - Block the request, until a connection returns to the " +
                                "pool.\n" +
                                "2 - Grow the connection pool size.",
                        type = {DataType.INT},
                        optional = true,
                        defaultValue = "1 (Block when exhausted)"),
                @Parameter(
                        name = "hostname.verification.enabled",
                        description = "Enable hostname verification.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "true"),
        },
        examples = {
                @Example(syntax = "" +
                        "@store(type='rdbms' , jdbc.url='jdbc:mysql://localhost:3306/production?useSSL=false', " +
                        "username='root', password='root', jdbc.driver.name='com.mysql.jdbc.Driver') \n" +
                        "@sink(type='websubhubeventpublisher', hub.id=\"anu_123\" , table.name='SessionTable'," +
                        "publisher.url=\"mysql://localhost:3306/production?useSSL=false\",\n" +
                        "@map(type='keyvalue',implicit.cast.enable='true'))\n" +
                        "define stream LowProductionAlertStream (topic string, payload string);",
                        description = "Subscribed users will received the messages generated through the hub " +
                                "and will publish to the callback url when subscribe. "
                )},
        systemParameter = {
                @SystemParameter(
                        name = "clientBootstrapClientGroupSize",
                        description = "Number of client threads to perform non-blocking read and write to " +
                                "one or more channels.",
                        defaultValue = "(Number of available processors) * 2",
                        possibleParameters = "Any positive integer"
                ),
                @SystemParameter(
                        name = "clientBootstrapBossGroupSize",
                        description = "Number of boss threads to accept incoming connections.",
                        defaultValue = "Number of available processors",
                        possibleParameters = "Any positive integer"
                ),
                @SystemParameter(
                        name = "clientBootstrapWorkerGroupSize",
                        description = "Number of worker threads to accept the connections from boss threads and " +
                                "perform non-blocking read and write from one or more channels.",
                        defaultValue = "(Number of available processors) * 2",
                        possibleParameters = "Any positive integer"
                ),
                @SystemParameter(
                        name = "trustStoreLocation",
                        description = "The default truststore file path.",
                        defaultValue = "`${carbon.home}/resources/security/client-truststore.jks`",
                        possibleParameters = "Path to client truststore `.jks` file"
                ),
                @SystemParameter(
                        name = "trustStorePassword",
                        description = "The default truststore password.",
                        defaultValue = "wso2carbon",
                        possibleParameters = "Truststore password as string"
                )
        }
)
public class WebSubHubSink extends Sink {

    private static final Logger log = Logger.getLogger(WebSubHubSink.class);
    private final String[] outputColumns = new String[]{HUB_CALLBACK, HUB_TOPIC, HUB_SECRET, HUB_LEASE_SECONDS,
            REQUEST_TIMESTAMP};
    protected String streamID;
    protected String consumerKey;
    protected String consumerSecret;
    protected ClientConnector staticClientConnector;
    protected SiddhiAppContext siddhiAppContext;
    protected String tokenURL;
    protected SinkMetrics metrics;
    protected long startTime;
    protected long endTime;
    String mapType;
    Option httpHeaderOption;
    Option httpMethodOption;
    String[] mandatoryColumns = new String[]{HUB_CALLBACK, HUB_LEASE_SECONDS, HUB_SECRET, HUB_TOPIC,
            HUB_MODE_COLUMN_NAME, HUB_ID_COLUMN_NAME, REQUEST_TIMESTAMP};
    private String clientStoreFile;
    private String clientStorePass;
    private int socketIdleTimeout;
    private String sslProtocol;
    private String tlsStoreType;
    private String chunkDisabled;
    private String parametersList;
    private String clientBootstrapConfiguration;
    private ConfigReader configReader;
    private String hostnameVerificationEnabled;
    private String sslVerificationDisabled;
    private Executor executor = null;
    private DefaultHttpWsConnectorFactory httpConnectorFactory;
    private ProxyServerConfiguration proxyServerConfiguration;
    private PoolConfiguration connectionPoolConfiguration;
    private String hubId;
    private Table subscriptionTable;
    private Map<String, List<WebSubSubscriptionDTO>> webSubSubscriptionMap;
    private ScheduledExecutorService scheduledExecutorService;
    private StreamDefinition outputStreamDefinition;
    private OnDemandQueryRuntime onDemandQueryRuntime;
    private long webSubSubscriptionMapUpdateTimeInterval;
    private List<WebSubSubscriptionDTO> expiredSubscriptions = new ArrayList<>();

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{Map.class};
    }

    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{HttpConstants.HEADERS, HttpConstants.METHOD, HttpConstants.PUBLISHER_URL,
                HttpConstants.RECEIVER_REFRESH_TOKEN};
    }

    @Override
    protected StateFactory init(StreamDefinition outputStreamDefinition, OptionHolder optionHolder,
                                ConfigReader sinkConfigReader, SiddhiAppContext siddhiAppContext) {
        //read configurations
        this.outputStreamDefinition = outputStreamDefinition;
        this.configReader = sinkConfigReader;
        this.siddhiAppContext = siddhiAppContext;
        this.streamID = siddhiAppContext.getName() + PORT_HOST_SEPARATOR + outputStreamDefinition.toString();
        this.httpHeaderOption = optionHolder.getOrCreateOption(HttpConstants.HEADERS, HttpConstants.DEFAULT_HEADER);
        this.httpMethodOption = optionHolder.getOrCreateOption(HttpConstants.METHOD, HttpConstants.DEFAULT_METHOD);
        this.consumerKey = optionHolder.validateAndGetStaticValue(HttpConstants.CONSUMER_KEY, EMPTY_STRING);
        this.consumerSecret = optionHolder.validateAndGetStaticValue(HttpConstants.CONSUMER_SECRET, EMPTY_STRING);
        this.tokenURL = optionHolder.validateAndGetStaticValue(HttpConstants.TOKEN_URL, EMPTY_STRING);
        this.clientStoreFile = optionHolder.validateAndGetStaticValue(HttpConstants.CLIENT_TRUSTSTORE_PATH_PARAM,
                HttpSinkUtil.trustStorePath(configReader));
        //create a static method to notify from source
        this.webSubSubscriptionMapUpdateTimeInterval = Long.parseLong(optionHolder.
                validateAndGetStaticValue(WEB_SUB_SUBSCRIPTION_MAP_UPDATE_TIMESTAMP, "0"));
        clientStorePass = optionHolder.validateAndGetStaticValue(HttpConstants.CLIENT_TRUSTSTORE_PASSWORD_PARAM,
                HttpSinkUtil.trustStorePassword(configReader));
        socketIdleTimeout = Integer.parseInt(optionHolder.validateAndGetStaticValue
                (HttpConstants.SOCKET_IDEAL_TIMEOUT, SOCKET_IDEAL_TIMEOUT_VALUE));
        sslProtocol = optionHolder.validateAndGetStaticValue(HttpConstants.SSL_PROTOCOL, EMPTY_STRING);
        tlsStoreType = optionHolder.validateAndGetStaticValue(HttpConstants.TLS_STORE_TYPE, EMPTY_STRING);
        chunkDisabled = optionHolder.validateAndGetStaticValue(HttpConstants.CLIENT_CHUNK_DISABLED, EMPTY_STRING);

        this.hubId = optionHolder.validateAndGetStaticValue(HUB_ID);
        String tableName = optionHolder.validateAndGetStaticValue(WEB_SUB_SUBSCRIPTION_DATA_TABLE_KEY);
        this.subscriptionTable = getSubscriptionTable(tableName);
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(10);
        setOnDemandQueryRuntimeForFindSubscription();

        //pool configurations
        connectionPoolConfiguration = createPoolConfigurations(optionHolder);
        parametersList = optionHolder.validateAndGetStaticValue(HttpConstants.SINK_PARAMETERS, EMPTY_STRING);
        clientBootstrapConfiguration = optionHolder
                .validateAndGetStaticValue(HttpConstants.CLIENT_BOOTSTRAP_CONFIGURATION, EMPTY_STRING);
        hostnameVerificationEnabled = optionHolder.validateAndGetStaticValue(
                HttpConstants.HOSTNAME_VERIFICATION_ENABLED, TRUE);
        sslVerificationDisabled = optionHolder.validateAndGetStaticValue(HttpConstants.SSL_VERIFICATION_DISABLED,
                FALSE);
        proxyServerConfiguration = createProxyServerConfiguration(optionHolder, streamID, siddhiAppContext.getName());
        httpConnectorFactory = createConnectorFactory(configReader);
        initMetrics(outputStreamDefinition.getId());
        return null;
    }

    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, State state)
            throws ConnectionUnavailableException {
        startTime = System.currentTimeMillis();
        Map<Object, Object> payloadMap = ((HashMap) (payload));
        Object topic = payloadMap.remove(HUB_TOPIC);
        List<WebSubSubscriptionDTO> subscriptionListToPublish;
        String headers = httpHeaderOption.getValue(dynamicOptions);
        List<Header> headersList = HttpSinkUtil.getHeaders(headers) != null ? HttpSinkUtil.getHeaders(headers) :
                new ArrayList<>();
        if (topic != null) {
            subscriptionListToPublish = this.webSubSubscriptionMap.get(topic.toString());
            if (subscriptionListToPublish != null) {
                for (WebSubSubscriptionDTO webSubDTO : subscriptionListToPublish) {
                    long subscriptionLeaseTime = webSubDTO.getLeaseSeconds();
                    long subscribedTime = webSubDTO.getTimestamp();
                    if (System.currentTimeMillis() < subscribedTime + subscriptionLeaseTime * 1000) {
                        if (!webSubDTO.getSecret().equalsIgnoreCase("")) {
                            String signatureHeaderValue = SHA256_HASHING +
                                    getHexValue(payloadMap.get("payload").toString());
                            Header signatureHeader = new Header(X_HUB_SIGNATURE, signatureHeaderValue);
                            headersList.add(signatureHeader);
                        }

                        String publisherURL = webSubDTO.getCallback();
                        ClientConnector clientConnector;
                        clientConnector = createClientConnector(publisherURL);

                        if (mapType == null) {
                            mapType = getMapper().getType();
                        }
                        sendRequest(payloadMap, dynamicOptions, headersList, clientConnector, publisherURL);
                    } else {
                        log.debug("Added to expired subscription list " + webSubDTO.getCallback() + " : "
                                + webSubDTO.getTopic());
                        expiredSubscriptions.add(webSubDTO);
                    }
                }
                for (WebSubSubscriptionDTO expiredSubscription : expiredSubscriptions) {
                    subscriptionListToPublish.remove(expiredSubscription);
                }
                webSubSubscriptionMap.replace(topic.toString(), subscriptionListToPublish);
            }
        }
    }

    protected int sendRequest(Object payload, DynamicOptions dynamicOptions, List<Header> headersList,
                              ClientConnector clientConnector, String publisherURL)
            throws ConnectionUnavailableException {
        String httpMethod = EMPTY_STRING.equals(httpMethodOption.getValue(dynamicOptions)) ?
                HttpConstants.METHOD_DEFAULT : httpMethodOption.getValue(dynamicOptions);
        String contentType = HttpSinkUtil.getContentType(mapType, headersList);
        String messageBody = getMessageBody(payload);
        HttpMethod httpReqMethod = new HttpMethod(httpMethod);
        HttpCarbonMessage cMessage = new HttpCarbonMessage(
                new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpReqMethod, EMPTY_STRING));
        cMessage = generateCarbonMessage(headersList, contentType, httpMethod, cMessage,
                clientConnector.getHttpURLProperties());

        if (metrics != null) {
            metrics.getTotalWritesMetric().inc();
            metrics.getTotalHttpWritesMetric(publisherURL).inc();
            metrics.getRequestSizeMetric(publisherURL).inc(HttpSinkUtil.getByteSize(messageBody));
        }

        if (!Constants.HTTP_GET_METHOD.equals(httpMethod)) {
            cMessage.addHttpContent(new DefaultLastHttpContent(Unpooled.wrappedBuffer(messageBody
                    .getBytes(Charset.defaultCharset()))));
        }
        cMessage.completeMessage();
        HttpResponseFuture responseFuture = clientConnector.send(cMessage);
        HTTPWebSubResponseListener responseListener = new HTTPWebSubResponseListener(payload, dynamicOptions, this,
                clientConnector.getPublisherURL());
        responseFuture.setHttpConnectorListener(responseListener);
        return HttpConstants.SUCCESS_CODE;
    }

    private String getStringFromInputStream(InputStream in) {
        BufferedInputStream bis = new BufferedInputStream(in);
        String result;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            int data;
            while ((data = bis.read()) != -1) {
                bos.write(data);
            }
            result = bos.toString(StandardCharsets.UTF_8.toString());
        } catch (IOException ioe) {
            log.error("Couldn't read the complete input stream due to: " + ioe.getMessage(), ioe);
            return "";
        }
        return result;
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        subscriptionTable.connectWithRetry();
        if (webSubSubscriptionMapUpdateTimeInterval != 0) {
            scheduledExecutorService.scheduleAtFixedRate(new SubscriptionMapUpdate(false,
                            onDemandQueryRuntime, hubId),
                    0, webSubSubscriptionMapUpdateTimeInterval, TimeUnit.SECONDS);
        } else {
            scheduledExecutorService.scheduleAtFixedRate(new SubscriptionMapUpdate(true,
                            onDemandQueryRuntime, hubId),
                    0, 1, TimeUnit.SECONDS);
        }
        scheduledExecutorService.scheduleAtFixedRate(
                new SubscriptionTableCleanupTask(subscriptionTable, siddhiAppContext), 0, 10, TimeUnit.SECONDS);
    }

    @Override
    public void disconnect() {
        if (staticClientConnector != null) {
            String publisherURL = staticClientConnector.getPublisherURL();
            staticClientConnector = null;
            log.debug("Server connector for url " + publisherURL + " disconnected.");
        }

        if (httpConnectorFactory != null) {
            httpConnectorFactory.shutdownNow();
            httpConnectorFactory = null;
        }
        scheduledExecutorService.shutdownNow();
    }

    @Override
    public void destroy() {
        if (staticClientConnector != null) {
            String publisherURL = staticClientConnector.getPublisherURL();
            staticClientConnector = null;
            log.debug("Server connector for url " + publisherURL + " disconnected.");
        }
    }

    /**
     * Initialize metrics.
     */
    protected void initMetrics(String streamName) {
        if (MetricsDataHolder.getInstance().getMetricService() != null
                && MetricsDataHolder.getInstance().getMetricManagementService().isEnabled()) {
            try {
                if (MetricsDataHolder.getInstance().getMetricManagementService()
                        .isReporterRunning(HttpConstants.PROMETHEUS_REPORTER_NAME)) {
                    metrics = new SinkMetrics(siddhiAppContext.getName(), streamName);
                }
            } catch (IllegalArgumentException e) {
                log.debug("Prometheus reporter is not running. Hence http sink metrics will not be initialized for "
                        + siddhiAppContext.getName());
            }
        }
    }

    /**
     * The method is responsible of generating carbon message to send.
     *
     * @param headers           the headers set.
     * @param contentType       the content type. Value is if user has to given it as a header or if not it is map type.
     * @param httpMethod        http method type.
     * @param cMessage          carbon message to be send to the endpoint.
     * @param httpURLProperties
     * @return generated carbon message.
     */
    HttpCarbonMessage generateCarbonMessage(List<Header> headers, String contentType,
                                            String httpMethod, HttpCarbonMessage cMessage,
                                            Map<String, String> httpURLProperties) {
        // Set protocol type http or https
        cMessage.setProperty(Constants.PROTOCOL, httpURLProperties.get(Constants.PROTOCOL));
        // Set uri
        cMessage.setProperty(Constants.TO, httpURLProperties.get(Constants.TO));
        // set Host
        cMessage.setProperty(Constants.HTTP_HOST, httpURLProperties.get(Constants.HTTP_HOST));
        //set port
        cMessage.setProperty(Constants.HTTP_PORT, Integer.valueOf(httpURLProperties.get(Constants.HTTP_PORT)));
        // Set method
        cMessage.setHttpMethod(httpMethod);
        //Set request URL
        cMessage.setRequestUrl(httpURLProperties.get(REQUEST_URL));
        HttpHeaders httpHeaders = cMessage.getHeaders();

        //Set request headers.
        httpHeaders.set(Constants.HTTP_HOST, cMessage.getProperty(Constants.HTTP_HOST));
        // Set user given Headers
        if (headers != null) {
            for (Header header : headers) {
                httpHeaders.set(header.getName(), header.getValue());
            }
        }
        // Set content type if content type is not included in headers
        if (contentType.contains(mapType)) {
            httpHeaders.set(HttpConstants.HTTP_CONTENT_TYPE, contentType);
        }
        //set method-type header
        cMessage.setHttpMethod(httpMethod);
        return cMessage;
    }

    String getMessageBody(Object payload) {
        if (HttpConstants.MAP_KEYVALUE.equals(mapType)) {
            Map<String, Object> params = (HashMap) payload;
            return params.get("payload").toString();
        } else {
            return (String) payload;
        }
    }

    public ClientConnector createClientConnector(String publisherURL) {

        String scheme = HttpSinkUtil.getScheme(publisherURL);
        Map<String, String> httpURLProperties = HttpSinkUtil.getURLProperties(publisherURL);
        //Generate basic sender configurations
        SenderConfiguration senderConfig = HttpSinkUtil
                .getSenderConfigurations(httpURLProperties, clientStoreFile, clientStorePass, configReader);
        if (EMPTY_STRING.equals(publisherURL)) {
            throw new SiddhiAppCreationException("Receiver URL found empty but it is Mandatory field in " +
                    "" + HttpConstants.HTTP_SINK_ID + " in " + streamID);
        }
        if (HttpConstants.SCHEME_HTTPS.equals(scheme) && ((clientStoreFile == null) || (clientStorePass == null))) {
            throw new ExceptionInInitializerError("Client trustStore file path or password are empty while " +
                    "default scheme is 'https'. Please provide client " +
                    "trustStore file path and password in " + streamID);
        }
        if (proxyServerConfiguration != null) {
            senderConfig.setProxyServerConfiguration(proxyServerConfiguration);
        }
        senderConfig.setPoolConfiguration(connectionPoolConfiguration);

        //add advanced sender configurations
        if (socketIdleTimeout != -1) {
            senderConfig.setSocketIdleTimeout(socketIdleTimeout);
        }
        if (!EMPTY_STRING.equals(sslProtocol)) {
            senderConfig.setSSLProtocol(sslProtocol);
        }
        if (!EMPTY_STRING.equals(tlsStoreType)) {
            senderConfig.setTLSStoreType(tlsStoreType);
        }
        if (!EMPTY_STRING.equals(chunkDisabled)) {
            if (chunkDisabled != null) {
                if (Boolean.parseBoolean(chunkDisabled)) {
                    senderConfig.setChunkingConfig(ChunkConfig.NEVER);
                } else {
                    senderConfig.setChunkingConfig(ChunkConfig.ALWAYS);
                }
            }
        }
        if (!EMPTY_STRING.equals(parametersList)) {
            senderConfig.setParameters(HttpIoUtil.populateParameters(parametersList));
        }
        if (!TRUE.equalsIgnoreCase(hostnameVerificationEnabled)) {
            senderConfig.setHostNameVerificationEnabled(false);
        }

        if (TRUE.equalsIgnoreCase(sslVerificationDisabled)) {
            senderConfig.disableSsl();
        }

        executor = Executors.newFixedThreadPool(
                senderConfig.getPoolConfiguration().getExecutorServiceThreads());

        //overwrite default transport configuration
        Map<String, Object> bootStrapProperties = HttpSinkUtil
                .populateTransportConfiguration(clientBootstrapConfiguration);
        return new ClientConnector(publisherURL, httpURLProperties,
                httpConnectorFactory.createHttpClientConnector(bootStrapProperties, senderConfig));
    }

    private String encodeMessage(Object s) {
        try {
            return URLEncoder.encode((String) s, HttpConstants.DEFAULT_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new SiddhiAppRuntimeException("Execution of Siddhi app " + siddhiAppContext.getName() +
                    " failed due to " + e.getMessage(), e);
        }
    }

    private Table getSubscriptionTable(String tableName) {
        TableDefinition tableDefinition = TableDefinition.id(tableName);
        List<Annotation> annotationsInSourceDefinition = outputStreamDefinition.getAnnotations();
        for (String column : mandatoryColumns) {
            if (column.equals(HUB_LEASE_SECONDS) || column.equals(REQUEST_TIMESTAMP)) {
                tableDefinition.attribute(column, Attribute.Type.LONG);
            } else {
                tableDefinition.attribute(column, Attribute.Type.STRING);
            }
        }
        annotationsInSourceDefinition.forEach(annotation -> {
            if (annotation.getName().equalsIgnoreCase(SiddhiConstants.ANNOTATION_STORE)) {
                tableDefinition.annotation(annotation);
            }
        });
        SiddhiAppRuntimeBuilder siddhiAppRuntimeBuilder = new SiddhiAppRuntimeBuilder(siddhiAppContext);
        siddhiAppRuntimeBuilder.defineTable(tableDefinition);
        siddhiAppRuntimeBuilder.getTableDefinitionMap().put(tableName, tableDefinition);
        return siddhiAppRuntimeBuilder.getTableMap().get(tableName);
    }

    private void setOnDemandQueryRuntimeForFindSubscription() {
        List<OutputAttribute> selectionList = new ArrayList<>();
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put(subscriptionTable.getTableDefinition().getId(), subscriptionTable);
        Map<String, Window> windowMap = new HashMap<>();
        Map<String, AggregationRuntime> aggregationRuntimeMap = new HashMap<>();

        for (String column : outputColumns) {
            selectionList.add(new OutputAttribute(new Variable(column)));
        }

        Selector selector = new Selector().addSelectionList(selectionList);

        InputStore inputStore = InputStore.store(subscriptionTable.getTableDefinition().getId()).on(new Compare(
                new StringConstant(hubId),
                Compare.Operator.EQUAL,
                new Variable(HUB_ID_COLUMN_NAME)));
        OnDemandQuery onDemandQuery = new OnDemandQuery().from(inputStore).select(selector);
        onDemandQuery.setType(OnDemandQuery.OnDemandQueryType.FIND);
        this.onDemandQueryRuntime = OnDemandQueryParser.parse(onDemandQuery, null,
                siddhiAppContext, tableMap, windowMap, aggregationRuntimeMap);
    }


    public void setWebSubSubscriptionMap(Map<String, List<WebSubSubscriptionDTO>> webSubSubscriptionMap) {
        this.webSubSubscriptionMap = webSubSubscriptionMap;
    }

    public void removeFromExpiredSubscriptions(WebSubSubscriptionDTO webSubSubscriptionDTO) {
        this.expiredSubscriptions.remove(webSubSubscriptionDTO);
    }

    String getHexValue(String payload) {
        return DigestUtils.sha256Hex(payload);
    }

    private class HTTPWebSubResponseListener implements HttpConnectorListener {
        private final String publisherURL;
        Object payload;
        DynamicOptions dynamicOptions;
        WebSubHubSink httpSink;

        HTTPWebSubResponseListener(Object payload, DynamicOptions dynamicOptions, WebSubHubSink httpSink,
                                   String publisherURL) {
            this.payload = payload;
            this.dynamicOptions = dynamicOptions;
            this.httpSink = httpSink;
            this.publisherURL = publisherURL;
        }

        @Override
        public void onMessage(HttpCarbonMessage httpCarbonMessage) {
            endTime = System.currentTimeMillis();

            if (metrics != null) {
                metrics.setEndpointStatusMetric(publisherURL, EndpointStatus.ONLINE);
                metrics.setLatencyMetric(publisherURL, endTime - startTime);
                metrics.setLastEventTime(publisherURL, endTime);

                // Catch unsuccessful requests
                if (httpCarbonMessage.getHttpStatusCode() / 100 != 2) {
                    metrics.getTotalHttpErrorsMetric(publisherURL).inc();
                }
            }
            if (executor != null) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                            /*
                            Read content from the input stream of the HTTP Carbon message
                            and make sure that the Carbon message is cleaned up,
                            for preventing leaks.
                             */
                        getStringFromInputStream(
                                new HttpMessageDataStreamer(httpCarbonMessage).getInputStream());
                    }
                });
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (metrics != null) {
                metrics.getTotalHttpErrorsMetric(publisherURL).inc();
                metrics.setEndpointStatusMetric(publisherURL, EndpointStatus.OFFLINE);
            }
            httpSink.onError(payload, dynamicOptions,
                    new ConnectionUnavailableException("HTTP sink on stream " + httpSink.streamID +
                            " of Siddhi App '" + httpSink.siddhiAppContext.getName() +
                            "' failed to publish events to endpoint '" + publisherURL + "'. " +
                            throwable.getMessage(), throwable));
        }
    }

    private class SubscriptionMapUpdate implements Runnable {
        boolean isStateCheck;
        OnDemandQueryRuntime onDemandQueryRuntime;
        String hubId;
        boolean initialExecution = true;

        SubscriptionMapUpdate(boolean isStateCheck, OnDemandQueryRuntime onDemandQueryRuntime, String hubId) {
            this.isStateCheck = isStateCheck;
            this.onDemandQueryRuntime = onDemandQueryRuntime;
            this.hubId = hubId;
        }

        @Override
        public void run() {
            boolean status;
            if (isStateCheck) {
                status = HttpIoUtil.isWebSubSinkUpdated(hubId);
                if (status || initialExecution) {
                    log.debug("Running SubscriptionMapUpdate task mode Status Check: " + isStateCheck + " status : "
                            + status);
                    updateSubscriptionMap();
                    initialExecution = false;
                }
            } else {
                updateSubscriptionMap();
            }
        }

        public void updateSubscriptionMap() {
            Map<String, List<WebSubSubscriptionDTO>> tempWebSubSubscriptionDTOMap = new HashMap<>();
            try {
                Event[] events = onDemandQueryRuntime.execute();
                if (events != null && events.length > 0) {
                    for (Event event : events) {
                        String topic = event.getData(1).toString();
                        List<WebSubSubscriptionDTO> webSubSubscriptionList = tempWebSubSubscriptionDTOMap.
                                getOrDefault(topic, new ArrayList<>());
                        webSubSubscriptionList.add(new WebSubSubscriptionDTO(hubId, event.getData(0),
                                topic, event.getData(2), event.getData(3), event.getData(4)));
                        tempWebSubSubscriptionDTOMap.put(topic, webSubSubscriptionList);
                    }
                }
                setWebSubSubscriptionMap(tempWebSubSubscriptionDTOMap);
            } catch (Exception e) {
                log.error("Error occurred while updating webSubSubscriptionMap ", e);
            }
        }
    }

    private class SubscriptionTableCleanupTask implements Runnable {
        CompiledCondition compiledCondition;

        SubscriptionTableCleanupTask(Table subscriptionTable, SiddhiAppContext siddhiAppContext) {
            Map<String, Table> tableMap = new HashMap<>();
            tableMap.put(subscriptionTable.getTableDefinition().getId(), subscriptionTable);
            SiddhiQueryContext siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext,
                    siddhiAppContext.getName());
            this.compiledCondition = HttpIoUtil.createTableDeleteResource(tableMap,
                    subscriptionTable.getTableDefinition().getId(), siddhiQueryContext);

        }

        @Override
        public void run() {
            List<WebSubSubscriptionDTO> deletedSubscriptions = new ArrayList<>();
            if (expiredSubscriptions.size() > 0) {
                for (WebSubSubscriptionDTO dto : expiredSubscriptions) {
                    log.debug("Deleting record " + dto.getCallback() + " : " + dto.getTopic());
                    try {
                        subscriptionTable.deleteEvents(generateComplexEvent(dto), compiledCondition, 1);

                        deletedSubscriptions.add(dto);
                    } catch (Throwable e) {
                        log.error("Error occurred while deleting expired subscription record from database" +
                                " table. callbackUrl :" + dto.getCallback() + ", Topic " + dto.getTopic(), e);
                        expiredSubscriptions.remove(dto);
                    }
                }
                log.info(deletedSubscriptions.size() + "Expired subscriptions deleted successfully.");
                deletedSubscriptions.forEach(dto -> expiredSubscriptions.remove(dto));
            }
        }

        private ComplexEventChunk generateComplexEvent(WebSubSubscriptionDTO dto) {
            Object[] event = new Object[]{dto.getCallback(), dto.getTopic()};
            StreamEvent complexEvent = new StreamEvent(0, 0, 2);
            StateEvent stateEvent = new StateEvent(1, 2);
            complexEvent.setOutputData(event);
            stateEvent.addEvent(0, complexEvent);
            stateEvent.setType(ComplexEvent.Type.CURRENT);
            ComplexEventChunk complexEventChunk = new ComplexEventChunk();
            complexEventChunk.add(stateEvent);
            return complexEventChunk;
        }
    }
}
