// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.sdk.iot.device.transport.mqtt;

import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.exceptions.TransportException;
import com.microsoft.azure.sdk.iot.device.transport.IotHubListener;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class MqttMessaging extends Mqtt
{
    private String moduleId;
    private String eventsSubscribeTopic;
    private String publishTopic;

    public MqttMessaging(MqttConnection mqttConnection, String deviceId, IotHubListener listener, MqttMessageListener messageListener, String connectionId, String moduleId, Map<Integer, Message> unacknowledgedSentMessages) throws TransportException
    {
        //Codes_SRS_MqttMessaging_25_002: [The constructor shall use the configuration to instantiate super class and passing the parameters.]
        super(mqttConnection, listener, messageListener, connectionId, unacknowledgedSentMessages);

        if (deviceId == null || deviceId.isEmpty())
        {
            //Codes_SRS_MqttMessaging_25_001: [The constructor shall throw IllegalArgumentException if any of the parameters are null or empty .]
            throw new IllegalArgumentException("Device id cannot be null or empty");
        }

        if (moduleId == null || moduleId.isEmpty())
        {
            //Codes_SRS_MqttMessaging_25_003: [The constructor construct publishTopic and eventsSubscribeTopic from deviceId.]
            this.publishTopic = "devices/" + deviceId + "/messages/events/";
            this.eventsSubscribeTopic = "devices/" + deviceId + "/messages/devicebound/#";
        }
        else
        {
            //Codes_SRS_MqttMessaging_34_031: [The constructor construct publishTopic and eventsSubscribeTopic from deviceId and moduleId.]
            this.publishTopic = "devices/" + deviceId + "/modules/" + moduleId +"/messages/events/";
            this.eventsSubscribeTopic = "devices/" + deviceId + "/modules/" + moduleId + "/messages/devicebound/#";
        }

        this.moduleId = moduleId;
    }

    public void start() throws TransportException
    {
        //Codes_SRS_MqttMessaging_25_020: [start method shall be call connect to establish a connection to IOT Hub with the given configuration.]
        this.connect();

        //Codes_SRS_MqttMessaging_34_035: [start method shall subscribe to the cloud to device events if not communicating to an edgeHub.]
        this.subscribe(this.eventsSubscribeTopic);

    }

    public void stop() throws TransportException
    {
        //Codes_SRS_MqttMessaging_25_022: [stop method shall be call disconnect to tear down a connection to IOT Hub with the given configuration.]
        this.disconnect();
    }

    /**
     * Sends the provided telemetry message over the mqtt connection
     *
     * @param message the message to send
     * @throws TransportException if any exception is encountered while sending the message
     */
    public void send(Message message) throws TransportException
    {
        if (message == null || message.getBytes() == null)
        {
            //Codes_SRS_MqttMessaging_25_025: [send method shall throw an IllegalArgumentException if the message is null.]
            throw new IllegalArgumentException("Message cannot be null");
        }

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(this.publishTopic);

        String messagePublishTopic = stringBuilder.toString();

        //Codes_SRS_MqttMessaging_25_024: [send method shall publish a message to the IOT Hub on the publish topic by calling method publish().]
        this.publish(messagePublishTopic, message);
    }
}
