/*
 *  Copyright (c) 2017 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.extension.siddhi.io.http.source;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.extension.siddhi.io.http.util.HttpConstants;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.transport.http.netty.message.HTTPCarbonMessage;
import org.wso2.transport.http.netty.message.HttpMessageDataStreamer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.stream.Collectors;

/**
 * Handles sending data to source listener.
 */
public class HttpResponseProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(HttpResponseProcessor.class);
    private HTTPCarbonMessage carbonMessage;
    private SourceEventListener sourceEventListener;
    private String sinkId;
    private String[] trpProperties;

    String filePath;
    String paylod = "{\"event\":{\"filePath\":\"%s\", \"fileName\":\"%s\"}}";

    HttpResponseProcessor(HTTPCarbonMessage cMessage, SourceEventListener sourceEventListener,
                          String sinkId, String[] trpProperties) {
        this.carbonMessage = cMessage;
        this.sourceEventListener = sourceEventListener;
        this.sinkId = sinkId;
        this.trpProperties = trpProperties;
    }
    
    @Override
    public void run() {
        boolean isDownloadableContent = (boolean) (carbonMessage.getProperty(HttpConstants.IS_DOWNLOADABLE_CONTENT));
        if (!isDownloadableContent) {
            BufferedReader buf = new BufferedReader(
                    new InputStreamReader(
                            new HttpMessageDataStreamer(carbonMessage).getInputStream(), Charset.defaultCharset()));
            try {
                String payload = buf.lines().collect(Collectors.joining("\n"));
                if (!payload.equals(HttpConstants.EMPTY_STRING)) {
                    sourceEventListener.onEvent(payload, trpProperties);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Submitted Event :" + payload);
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Empty payload event, hence dropping the event chunk in " + sinkId);
                    }
                }
            } finally {
                try {
                    buf.close();
                } catch (IOException e) {
                    logger.error("Error closing byte buffer.", e);
                }
            }
        } else {
            filePath = (String) carbonMessage.getProperty(HttpConstants.DESTINATION_PATH);
            writeToTile(carbonMessage);
            sourceEventListener.onEvent(String.format(paylod, filePath, getFileName()), trpProperties);
        }
    }

    private void writeToTile(HTTPCarbonMessage carbonMessage) {
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            inputStream = new HttpMessageDataStreamer(carbonMessage).getInputStream();
            outputStream = new FileOutputStream(new File(filePath));
            int read;
            byte[] bytes = new byte[1024];
            while ((read = inputStream.read(bytes)) != -1) {
                outputStream.write(bytes, 0, read);
            }
        } catch (FileNotFoundException e) {
            logger.error("Given path to download the file : '" + filePath + "' cannot be found.", e);
        } catch (IOException e) {
            logger.error("Error occured during writing the file to '" + filePath + "' due to " + e.getMessage(), e);
        } finally {
            try {
                inputStream.close();
                outputStream.close();
            } catch (IOException e) {
                logger.error("Failed to close the stream due to " + e.getMessage(), e);
            }
        }
    }

    private String getFileName() {
        try {
            return FilenameUtils.getName(new URL(filePath).getPath());
        } catch (MalformedURLException e) {
            logger.error("Given file path '" + filePath + "' is malformed.", e);
        }
        return null;
    }
}