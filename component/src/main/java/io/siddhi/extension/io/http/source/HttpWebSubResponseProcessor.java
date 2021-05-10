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

import io.netty.handler.codec.http.QueryStringDecoder;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.stream.input.source.SourceEventListener;
import io.siddhi.core.table.CompiledUpdateSet;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.extension.io.http.metrics.SourceMetrics;
import io.siddhi.extension.io.http.source.util.HttpSourceUtil;
import io.siddhi.extension.io.http.util.HttpConstants;
import io.siddhi.extension.io.http.util.HttpIoUtil;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.transport.http.netty.message.HttpCarbonMessage;
import org.wso2.transport.http.netty.message.HttpMessageDataStreamer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.siddhi.extension.io.http.util.HttpConstants.HUB;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_CALLBACK;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_LEASE_SECONDS;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_LEASE_SECOND_COLUMN_NAME;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_MODE;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_MODE_SUBSCRIBE;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_SECRET;
import static io.siddhi.extension.io.http.util.HttpConstants.HUB_TOPIC;
import static io.siddhi.extension.io.http.util.HttpConstants.INTERNAL_SERVER_FAIL_CODE;
import static io.siddhi.extension.io.http.util.HttpConstants.REQUEST_TIMESTAMP;


/**
 * Web Sub Hub incoming subscription request processor
 **/
public class HttpWebSubResponseProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(HttpWebSubResponseProcessor.class);
    private final HttpCarbonMessage carbonMessage;
    private final SourceEventListener sourceEventListener;
    private final String sourceID;
    private final Object[] trpProperties;
    private final SourceMetrics metrics;
    private final Table table;
    private final String hubId;
    private final SiddhiQueryContext siddhiQueryContext;
    private final Map<String, Table> tableMap;
    private final AddingStreamEventExtractor addingStreamEventExtractor;
    private final List<String> topics;
    private CompiledCondition updateCompileCondition;
    private UpdateSet updateSet;
    private CompiledUpdateSet compiledUpdateSet;
    private CompiledCondition deleteCompileCondition;

    HttpWebSubResponseProcessor(HttpCarbonMessage cMessage, SourceEventListener sourceEventListener,
                                String sourceID, String[] trpProperties, SourceMetrics metrics, Table table,
                                String hubId, SiddhiAppContext siddhiAppContext, List<String> topics) {
        this.carbonMessage = cMessage;
        this.sourceEventListener = sourceEventListener;
        this.sourceID = sourceID;
        this.trpProperties = trpProperties;
        this.metrics = metrics;
        this.table = table;
        this.hubId = hubId;
        this.siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, sourceID);
        Map<String, Table> tableMap = new HashMap<>();
        tableMap.put(table.getTableDefinition().getId(), table);
        this.tableMap = tableMap;
        createTableUpdateResources();
        this.deleteCompileCondition = HttpIoUtil.createTableDeleteResource(tableMap,
                table.getTableDefinition().getId(), siddhiQueryContext);
        this.addingStreamEventExtractor = new AddingStreamEventExtractor(0);
        this.topics = topics;
    }

    @Override
    public void run() {
        BufferedReader buf = new BufferedReader(
                new InputStreamReader(
                        new HttpMessageDataStreamer(carbonMessage).getInputStream(), Charset.defaultCharset()));
        try {
            String payload = buf.lines().collect(Collectors.joining("\n"));

            if (!payload.equals(HttpConstants.EMPTY_STRING)) {
                if (metrics != null) {
                    metrics.getTotalReadsMetric().inc();
                    metrics.getTotalHttpReadsMetric().inc();
                    metrics.getRequestSizeMetric().inc(HttpSourceUtil.getByteSize(payload));
                    metrics.setLastEventTime(System.currentTimeMillis());
                }

                Map<String, Object> payloadMap = processPayload(payload);
                boolean isRequestValid = HttpIoUtil.validateAndVerifySubscriptionRequest(carbonMessage, payloadMap,
                        payload, topics);
                if (isRequestValid) {
                    List<Object> event = new ArrayList<>();
                    List<Attribute> attributeList = table.getTableDefinition().getAttributeList();
                    if (payloadMap.get(HUB_MODE).toString().equalsIgnoreCase(HUB_MODE_SUBSCRIBE)) {
                        for (Attribute attribute : attributeList) {
                            if (attribute.getName().equalsIgnoreCase(HUB_CALLBACK)){
                                event.add(java.net.URLDecoder.decode(payloadMap.get(attribute.getName()).toString(),
                                        StandardCharsets.UTF_8.name()));
                            } else if (attribute.getName().equals(HttpConstants.HUB_ID_COLUMN_NAME)) {
                                event.add(hubId);
                            } else if (attribute.getName().equals(REQUEST_TIMESTAMP)) {
                                event.add(System.currentTimeMillis());
                            } else {
                                event.add(processAndGet(payloadMap.get(attribute.getName()).toString(), attribute));
                            }
                        }
                    } else {
                        event.add(payloadMap.get(HUB_CALLBACK));
                        event.add(payloadMap.get(HUB_TOPIC));
                    }
                    ComplexEventChunk eventChunk = new ComplexEventChunk();
                    StreamEvent complexEvent;
                    if (payloadMap.get(HUB_MODE).toString().equalsIgnoreCase(HUB_MODE_SUBSCRIBE)) {
                        complexEvent = new StreamEvent(0, 0,
                                attributeList.size());
                        StateEvent stateEvent = new StateEvent(1, 0);
                        complexEvent.setOutputData(event.toArray());
                        stateEvent.addEvent(0, complexEvent);
                        stateEvent.setType(ComplexEvent.Type.CURRENT);
                        eventChunk.add(stateEvent);
                        table.updateOrAddEvents(eventChunk, this.updateCompileCondition, compiledUpdateSet,
                                addingStreamEventExtractor, 1);
                    } else {
                        complexEvent = new StreamEvent(0, 0, 2);
                        StateEvent stateEvent = new StateEvent(1, 2);
                        complexEvent.setOutputData(event.toArray());
                        stateEvent.addEvent(0, complexEvent);
                        stateEvent.setType(ComplexEvent.Type.CURRENT);
                        eventChunk.add(stateEvent);
                        table.deleteEvents(eventChunk, deleteCompileCondition, 1);
                    }
                    logger.debug("Incoming Request accepted for callback: " + payloadMap.get(HUB_CALLBACK) +
                            ". topic: " + payloadMap.get(HUB_TOPIC) + " mode: " + payloadMap.get(HUB_MODE));
                    HttpIoUtil.notifyWebSubSink(hubId);
                    HttpIoUtil.handleResponse(carbonMessage, HttpIoUtil.createResponseMessageForWebSub(carbonMessage));
                    sourceEventListener.onEvent(payloadMap, trpProperties);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Submitted Event " + payload + " Stream");
                }
            } else {
                if (metrics != null) {
                    metrics.getTotalHttpErrorsMetric().inc();
                }
                HttpSourceUtil.handleCallback(carbonMessage, 405);
                if (logger.isDebugEnabled()) {
                    logger.debug("Empty payload event, hence dropping the event chunk at source " + sourceID);
                }
            }
        } catch (RuntimeException | UnsupportedEncodingException e) {
            HttpIoUtil.handleFailure(carbonMessage, null, INTERNAL_SERVER_FAIL_CODE, e.getMessage());
            logger.error("Error occurred while processing the payload ", e);
        } finally {
            try {
                buf.close();
                carbonMessage.waitAndReleaseAllEntities();
            } catch (IOException e) {
                if (metrics != null) {
                    metrics.getTotalHttpErrorsMetric().inc();
                }
                logger.error("Error occurred when closing the byte buffer in source " + sourceID, e);
            }
        }
    }

    public Map<String, Object> processPayload(String payload) {
        String decodedPayload = QueryStringDecoder.decodeComponent(payload, StandardCharsets.UTF_8);
        String[] parametersArray = decodedPayload.split("&");
        Map<String, Object> parameterMap = new HashMap<>();
        if (parametersArray.length > 0) {
            for (String parameterPair : parametersArray) {
                String[] parameterPairArray = parameterPair.split("=");
                if (parameterPairArray.length == 2) {
                    parameterMap.put(parameterPairArray[0].replace(HUB + ".", "").trim(),
                            parameterPairArray[1].trim());
                }
            }
        }
        if (!parameterMap.containsKey(HUB_SECRET)) {
            parameterMap.put(HUB_SECRET, "");
        } else if (!parameterMap.containsKey(HUB_LEASE_SECONDS)) {
            parameterMap.put(HUB_LEASE_SECONDS, 86400);
        }
        return parameterMap;
    }

    private Object processAndGet(String value, Attribute attribute) {
        switch (attribute.getType()) {
            default:
            case STRING:
                return value != null ? value : "N/A";
            case INT:
                return Integer.parseInt(value);
            case BOOL:
                return Boolean.parseBoolean(value);
            case LONG:
                if (attribute.getName().equalsIgnoreCase(HUB_LEASE_SECOND_COLUMN_NAME)) {
                    return Long.parseLong(value);
                }
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
        }
    }

    private void createTableUpdateResources() {
        Expression condition = HttpIoUtil.generateFilterConditionForWebSubHub(table);

        MetaStateEvent tableUpdateMetaStateEvent = new MetaStateEvent(2);

        MetaStreamEvent tableMetaStreamEvent = new MetaStreamEvent();
        MetaStreamEvent inputStreamMetaStreamEvent = new MetaStreamEvent();
        //generate table definition for the input stream
        TableDefinition inputTableDefinition = TableDefinition.id("");
        table.getTableDefinition().getAttributeList().forEach((
                attribute -> inputTableDefinition.attribute(attribute.getName(), attribute.getType())));
        //add input table definition to meta Stream event
        inputStreamMetaStreamEvent.addInputDefinition(inputTableDefinition);
        tableMetaStreamEvent.addInputDefinition(table.getTableDefinition());
        //set output data parameters to table and input meta stream event object
        table.getTableDefinition().getAttributeList().forEach(tableMetaStreamEvent::addOutputData);
        table.getTableDefinition().getAttributeList().forEach(inputStreamMetaStreamEvent::addOutputData);

        tableMetaStreamEvent.setEventType(MetaStreamEvent.EventType.TABLE);
        inputStreamMetaStreamEvent.setEventType(MetaStreamEvent.EventType.TABLE);

        tableUpdateMetaStateEvent.addEvent(inputStreamMetaStreamEvent);
        tableUpdateMetaStateEvent.addEvent(tableMetaStreamEvent);

        MatchingMetaInfoHolder matchingMetaInfoHolder = new MatchingMetaInfoHolder(tableUpdateMetaStateEvent, 0, 1,
                inputTableDefinition, table.getTableDefinition(), 0);

        this.updateCompileCondition = table.compileCondition(condition, matchingMetaInfoHolder, null,
                tableMap, siddhiQueryContext);

        this.updateSet = new UpdateSet();
        table.getTableDefinition().getAttributeList().forEach((
                attribute -> updateSet.set(new Variable(attribute.getName()), new Variable(attribute.getName()))));

        this.compiledUpdateSet = table.compileUpdateSet(updateSet, matchingMetaInfoHolder, null,
                tableMap, siddhiQueryContext);
    }
}
