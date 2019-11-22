// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.sdk.iot.device.transport.mqtt;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.exceptions.TransportException;
import com.microsoft.azure.sdk.iot.device.transport.*;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;


public class MqttIotHubConnection implements IotHubTransportConnection, MqttMessageListener
{
    /** The MQTT connection lock. */
    private final Object MQTT_CONNECTION_LOCK = new Object();

    private DeviceClientConfig config = null;
    private IotHubConnectionStatus state = IotHubConnectionStatus.DISCONNECTED;

    private String iotHubUserName;
    private String iotHubUserPassword;
    private MqttConnection mqttConnection;

    //string constants
    private static final String WS_SSL_PREFIX = "wss://";

    private static final String WEBSOCKET_RAW_PATH = "/$iothub/websocket";
    private static final String WEBSOCKET_QUERY = "?iothub-no-client-cert=true";

    private static final String SSL_PREFIX = "ssl://";
    private static final String SSL_PORT_SUFFIX = ":8883";

    private String connectionId;

    private IotHubListener listener;

    //Messaging clients
    private MqttMessaging deviceMessaging;

    private Map<IotHubTransportMessage, Integer> receivedMessagesToAcknowledge = new ConcurrentHashMap<>();
    private Map<Integer, Message> unacknowledgedSentMessages = new ConcurrentHashMap<>();

    /**
     * Constructs an instance from the given {@link DeviceClientConfig}
     * object.
     *
     * @param config the client configuration.
     */
    public MqttIotHubConnection(DeviceClientConfig config) throws IllegalArgumentException
    {
        synchronized (MQTT_CONNECTION_LOCK) {
            this.config = config;
            this.deviceMessaging = null;
        }
    }

    /**
     * Establishes a connection for the device and IoT Hub given in the client
     * configuration. If the connection is already open, the function shall do
     * nothing.
     *
     * @throws TransportException if a connection could not to be established.
     */
    public void open(DeviceClientConfig deviceClientConfigs, ScheduledExecutorService scheduledExecutorService) throws TransportException
    {
        connectionId = UUID.randomUUID().toString();

        synchronized (MQTT_CONNECTION_LOCK)
        {
            //Codes_SRS_MQTTIOTHUBCONNECTION_15_006: [If the MQTT connection is already open,
            // the function shall do nothing.]
            if (this.state == IotHubConnectionStatus.CONNECTED)
            {
                return;
            }

            System.out.println("Opening MQTT connection...");

            // Codes_SRS_MQTTIOTHUBCONNECTION_15_004: [The function shall establish an MQTT connection
            // with an IoT Hub using the provided host name, user name, device ID, and sas token.]
            try
            {
                mqttConnection = new MqttConnection(
                        config.getHostUrl(),
                        config.getClientID(),
                        config.getUsername(),
                        config.getPassword(),
                        null);


                //Codes_SRS_MQTTIOTHUBCONNECTION_34_030: [This function shall instantiate this object's MqttMessaging object with this object as the listener.]
                this.deviceMessaging = new MqttMessaging(mqttConnection, config.getClientID(), this.listener, this, this.connectionId, "getModuleId001", unacknowledgedSentMessages);
                this.mqttConnection.setMqttCallback(this.deviceMessaging);

                this.deviceMessaging.start();
                this.state = IotHubConnectionStatus.CONNECTED;

                System.out.println("MQTT connection opened successfully");

                //Codes_SRS_MQTTIOTHUBCONNECTION_34_065: [If the connection opens successfully, this function shall notify the listener that connection was established.]
                this.listener.onConnectionEstablished(this.connectionId);
            }
            catch (IOException e)
            {
                System.out.println("Exception encountered while opening MQTT connection; closing connection" + e);
                this.state = IotHubConnectionStatus.DISCONNECTED;

                if (this.deviceMessaging != null)
                {
                    this.deviceMessaging.stop();
                }
                throw new TransportException(e);
            }
        }
    }

    /**
     * Closes the connection. After the connection is closed, it is no longer usable.
     * If the connection is already closed, the function shall do nothing.
     */
    public void close() throws TransportException
    {
        // Codes_SRS_MQTTIOTHUBCONNECTION_15_007: [If the MQTT session is closed, the function shall do nothing.]
        if (this.state == IotHubConnectionStatus.DISCONNECTED)
        {
            return;
        }

        System.out.println("Closing MQTT connection");

        // Codes_SRS_MQTTIOTHUBCONNECTION_15_006: [The function shall close the MQTT connection.]
        try
        {
            if (this.deviceMessaging != null)
            {
                this.deviceMessaging.stop();
                this.deviceMessaging = null;
            }

            this.state = IotHubConnectionStatus.DISCONNECTED;
            System.out.println("Successfully closed MQTT connection");
        }
        catch (TransportException e)
        {
            //Codes_SRS_MQTTIOTHUBCONNECTION_34_021: [If a TransportException is encountered while closing the three clients, this function shall set this object's state to closed and then rethrow the exception.]
            this.state = IotHubConnectionStatus.DISCONNECTED;
            System.out.println("Exception encountered while closing MQTT connection, connection state is unknown" + e);
            throw e;
        }
    }

    /**
     * Receives a message, if one exists.
     *
     * @return the message received, or null if none exists.
     *
     * @throws TransportException if the connection state is currently closed.
     */
    private IotHubTransportMessage receiveMessage() throws TransportException
    {
        IotHubTransportMessage message;

        message = deviceMessaging.receive();
        if (message != null)
        {
            System.out.println("Received MQTT device messaging message ({})" +  message);
            return message;
        }

        return null;
    }

    @Override
    public void setListener(IotHubListener listener) throws IllegalArgumentException
    {
        if (listener == null)
        {
            //Codes_SRS_MQTTIOTHUBCONNECTION_34_049: [If the provided listener object is null, this function shall throw an IllegalArgumentException.]
            throw new IllegalArgumentException("listener cannot be null");
        }

        //Codes_SRS_MQTTIOTHUBCONNECTION_34_050: [This function shall save the provided listener object.]
        this.listener = listener;
    }

    /**
     * Sends an event message.
     *
     * @param message the event message.
     *
     * @return the status code from sending the event message.
     *
     * @throws TransportException if the MqttIotHubConnection is not open
     */
    @Override
    public IotHubStatusCode sendMessage(Message message) throws TransportException
    {
        synchronized (MQTT_CONNECTION_LOCK)
        {
            // Codes_SRS_MQTTIOTHUBCONNECTION_15_010: [If the message is null or empty,
            // the function shall return status code BAD_FORMAT.]
            if (message == null || message.getBytes() == null ||
                    message.getBytes().length == 0)
            {
                return IotHubStatusCode.BAD_FORMAT;
            }

            // Codes_SRS_MQTTIOTHUBCONNECTION_15_013: [If the MQTT connection is closed, the function shall throw an IllegalStateException.]
            if (this.state == IotHubConnectionStatus.DISCONNECTED)
            {
                throw new IllegalStateException("Cannot send event using a closed MQTT connection");
            }

            // Codes_SRS_MQTTIOTHUBCONNECTION_15_008: [The function shall send an event message
            // to the IoT Hub given in the configuration.]
            // Codes_SRS_MQTTIOTHUBCONNECTION_15_011: [If the message was successfully received by the service,
            // the function shall return status code OK_EMPTY.]
            IotHubStatusCode result = IotHubStatusCode.OK_EMPTY;


            System.out.println("Sending MQTT device telemetry message ({})" + message);
            this.deviceMessaging.send(message);

            return result;
        }
    }

    /**
     * Sends an ACK to the service for the provided message
     * @param message the message to acknowledge to the service
     * @return true if the ACK was sent successfully and false otherwise
     * @throws TransportException if the ACK could not be sent successfully
     */
    @Override
    public boolean sendMessageResult(Message message) throws TransportException
    {
        if (message == null)
        {
            //Codes_SRS_MQTTIOTHUBCONNECTION_34_057: [If the provided message or result is null, this function shall throw a TransportException.]
            throw new TransportException(new IllegalArgumentException("message and result must be non-null"));
        }

        int messageId;
        System.out.println("Checking if MQTT layer can acknowledge the received message: " + message);
        if (receivedMessagesToAcknowledge.containsKey(message))
        {
            //Codes_SRS_MQTTIOTHUBCONNECTION_34_052: [If this object has received the provided message from the service, this function shall retrieve the Mqtt messageId for that message.]
            messageId = receivedMessagesToAcknowledge.get(message);
        }
        else
        {
            TransportException e = new TransportException(new IllegalArgumentException("Provided message cannot be acknowledged because it was already acknowledged or was never received from service"));
            System.out.println("Mqtt layer could not acknowledge received message because it has no mapping to an outstanding mqtt message id ({})" + message + e);
            //Codes_SRS_MQTTIOTHUBCONNECTION_34_051: [If this object has not received the provided message from the service, this function shall throw a TransportException.]
            throw e;
        }

        boolean ackSent;
        System.out.println("Sending MQTT ACK for a received message: " + message);

        //Codes_SRS_MQTTIOTHUBCONNECTION_34_055: [If the provided message has message type other than DEVICE_METHODS and DEVICE_TWIN, this function shall invoke the telemetry client to send the ack and return the result.]
        ackSent = this.deviceMessaging.sendMessageAcknowledgement(messageId);


        if (ackSent)
        {
            //Codes_SRS_MQTTIOTHUBCONNECTION_34_056: [If the ack was sent successfully, this function shall remove the provided message from the saved map of messages to acknowledge.]
            System.out.println("MQTT ACK was sent for a received message so it has been removed from the messages to acknowledge list ({})" + message);
            this.receivedMessagesToAcknowledge.remove(message);
        }

        return ackSent;
    }

    @Override
    public String getConnectionId()
    {
        //Codes_SRS_MQTTIOTHUBCONNECTION_34_064: [This function shall return the saved connectionId.]
        return this.connectionId;
    }

    @Override
    public void onMessageArrived(int messageId)
    {
        IotHubTransportMessage transportMessage = null;
        try
        {
            //Codes_SRS_MQTTIOTHUBCONNECTION_34_058: [This function shall attempt to receive a message.]
            transportMessage = this.receiveMessage();
        }
        catch (TransportException e)
        {
            this.listener.onMessageReceived(null, new TransportException("Failed to receive message from service", e));
            System.out.println("Encountered exception while receiving message over MQTT" + e);
        }

        if (transportMessage == null)
        {
            //Ack is not sent to service for this message because we cannot interpret the message. Service will likely re-send
            this.listener.onMessageReceived(null, new TransportException("Message sent from service could not be parsed"));
            System.out.println("Received message that could not be parsed. That message has been ignored.");
        }
        else
        {
            //Codes_SRS_MQTTIOTHUBCONNECTION_34_059: [If a transport message is successfully received, this function shall save it in this object's map of messages to be acknowledged along with the provided messageId.]
            System.out.println("MQTT received message so it has been added to the messages to acknowledge list ({})" + transportMessage);
            System.out.println("payload2: " + Arrays.toString(transportMessage.getBytes()));
            this.receivedMessagesToAcknowledge.put(transportMessage, messageId);

            //Codes_SRS_MQTTIOTHUBCONNECTION_34_062: [If a transport message is successfully received, and the message has a type of DEVICE_TELEMETRY, this function shall set the callback and callback context of this object from the saved values in config for telemetry.]
            transportMessage.setMessageCallback(this.config.getDeviceTelemetryMessageCallback());
            transportMessage.setMessageCallbackContext(this.config.getDeviceTelemetryMessageContext());


            //Codes_SRS_MQTTIOTHUBCONNECTION_34_063: [If a transport message is successfully received, this function shall notify its listener that a message was received and provide the received message.]
            this.listener.onMessageReceived(transportMessage, null);
        }
    }
}
