/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.tagsync.process;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.atlas.ApplicationProperties;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.ranger.plugin.util.RangerRESTClient;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class HealthCheckService implements HttpHandler {

    private static final Log LOG = LogFactory.getLog(HealthCheckService.class);

    Properties props = null;
    Configuration conf = null;

    public HealthCheckService(Properties props) {
        this.props = props;
        try {
            conf = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.error("Error while reading atlas conf", e);
        }
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {

        if ("GET".equals(httpExchange.getRequestMethod())) {
            JSONObject json = new JSONObject();

            try {
                boolean ret = listKafkaTopics(conf);


                boolean isRangerUp = checkRangerHealth(props);

                try {
                    json.put("Kafka", ret);
                    json.put("Ranger", isRangerUp);
                } catch (JSONException e) {
                    LOG.error("Error while json ", e);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            handleResponse(httpExchange,  json.toString());
        }
    }


    private void handleResponse(HttpExchange httpExchange, String requestParamValue) throws IOException {

        OutputStream outputStream = httpExchange.getResponseBody();
        String htmlResponse = requestParamValue.toString();
        httpExchange.sendResponseHeaders(200, htmlResponse.length());
        outputStream.write(htmlResponse.getBytes());

        outputStream.flush();
        outputStream.close();

    }

    static boolean listKafkaTopics(Configuration configs) {

        Map<String, List<PartitionInfo>> topics = null;
        boolean ret = false;
        Properties props = new Properties();
        try {
            props.put("bootstrap.servers", configs.getProperty("atlas.kafka.bootstrap.servers"));
            props.put("group.id", configs.getProperty("atlas.kafka.entities.group.id"));
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
            topics = consumer.listTopics();
            LOG.info("topics" + topics);
            consumer.close();
        } catch (Exception e) {
            ret = false;
        }

        if (topics != null && topics.size() > 0) {
            ret = true;
        } else {
            ret = false;
        }
        return ret;
    }

    static boolean checkRangerHealth(Properties properties) throws Exception {

        String url = properties.getProperty("ranger.tagsync.dest.ranger.endpoint");
        String username = properties.getProperty("ranger.tagsync.dest.ranger.username");
        String password = properties.getProperty("ranger.tagsync.dest.ranger.password");
        String sslConfigFile = TagSyncConfig.getTagAdminRESTSslConfigFile(properties);

        RangerRESTClient restClient = new RangerRESTClient(url, sslConfigFile, TagSyncConfig.getInstance());
        restClient.setBasicAuthInfo(username, password);
        ClientResponse response = restClient.get("/tags/tagdefs", null, null);
        LOG.info("response ==>>" + response);

        return (response.getStatus() == ClientResponse.Status.OK.getStatusCode());
    }
}



