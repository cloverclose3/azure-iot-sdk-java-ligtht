// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.sdk.iot.device.transport.mqtt;

import com.microsoft.azure.sdk.iot.device.Message;
import com.microsoft.azure.sdk.iot.device.exceptions.TransportException;
import com.microsoft.azure.sdk.iot.device.transport.IotHubListener;
import com.microsoft.azure.sdk.iot.device.transport.IotHubTransportMessage;
import com.microsoft.azure.sdk.iot.device.transport.ReconnectionNotifier;
import com.microsoft.azure.sdk.iot.device.transport.mqtt.exceptions.PahoExceptionTranslator;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.paho.client.mqttv3.*;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

abstract public class Mqtt implements MqttCallback
{
    private static final int CONNECTION_TIMEOUT = 60 * 1000;
    private static final int DISCONNECTION_TIMEOUT = 60 * 1000;

    private MqttConnection mqttConnection;
    private MqttMessageListener messageListener;
    ConcurrentLinkedQueue<Pair<String, byte[]>> allReceivedMessages;
    private final Object stateLock;
    protected final Object incomingLock;
    private final Object publishLock;

    private Map<Integer, Message> unacknowledgedSentMessages;

    // SAS token expiration check on retry
    private boolean userSpecifiedSASTokenExpiredOnRetry = false;

    private IotHubListener listener;
    private String connectionId;

    /**
     * Constructor to instantiate mqtt broker connection.
     * @param mqttConnection the connection to use
     * @param listener the listener to be called back upon connection established/lost and upon a message being delivered
     * @param messageListener the listener to be called back upon a message arriving
     * @param connectionId the id of the connection
     * @throws IllegalArgumentException if the provided mqttConnection is null
     */
    public Mqtt(MqttConnection mqttConnection, IotHubListener listener, MqttMessageListener messageListener, String connectionId, Map<Integer, Message> unacknowledgedSentMessages) throws IllegalArgumentException
    {
        if (mqttConnection == null)
        {
            //Codes_SRS_Mqtt_25_002: [The constructor shall throw an IllegalArgumentException if mqttConnection is null.]
            throw new IllegalArgumentException("Mqtt connection info cannot be null");
        }

        //Codes_SRS_Mqtt_25_003: [The constructor shall retrieve lock, queue from the provided connection information and save the connection.]
        this.mqttConnection = mqttConnection;
        this.allReceivedMessages = mqttConnection.getAllReceivedMessages();
        this.stateLock = mqttConnection.getMqttLock();
        this.incomingLock = new Object();
        this.publishLock = new Object();
        this.userSpecifiedSASTokenExpiredOnRetry = false;
        this.listener = listener;
        this.messageListener = messageListener;
        this.connectionId = connectionId;
        this.unacknowledgedSentMessages = unacknowledgedSentMessages;
    }

    /**
     * Method to connect to mqtt broker connection.
     *
     * @throws TransportException if failed to establish the mqtt connection.
     */
    protected void connect() throws TransportException
    {
        synchronized (this.stateLock)
        {
            try
            {
                //Codes_SRS_Mqtt_25_008: [If the MQTT connection is already open, the function shall do nothing.]
                if (!this.mqttConnection.getMqttAsyncClient().isConnected())
                {
                    System.out.println("Sending MQTT CONNECT packet...");
                    //Codes_SRS_Mqtt_25_005: [The function shall establish an MQTT connection with an IoT Hub using the provided host name, user name, device ID, and sas token.]
                    IMqttToken connectToken = this.mqttConnection.getMqttAsyncClient().connect(Mqtt.this.mqttConnection.getConnectionOptions());
                    connectToken.waitForCompletion(CONNECTION_TIMEOUT);
                    System.out.println("Sent MQTT CONNECT packet was acknowledged");
                }
            }
            catch (MqttException e)
            {
                System.out.println("Exception encountered while sending MQTT CONNECT packet" +  e);

                this.disconnect();
                //Codes_SRS_Mqtt_34_044: [If an MqttException is encountered while connecting, this function shall throw the associated ProtocolException.]
                throw PahoExceptionTranslator.convertToMqttException(e, "Unable to establish MQTT connection");
            }
        }
    }

    /**
     * Method to disconnect to mqtt broker connection.
     *
     * @throws TransportException if failed to ends the mqtt connection.
     */
    protected void disconnect() throws TransportException
    {
        try
        {
            if (this.mqttConnection.isConnected())
            {
                System.out.println("Sending MQTT DISCONNECT packet");
                //Codes_SRS_Mqtt_34_055: [If an MQTT connection is connected, the function shall disconnect that connection.]
                IMqttToken disconnectToken = this.mqttConnection.disconnect();

                if (disconnectToken != null)
                {
                    disconnectToken.waitForCompletion(DISCONNECTION_TIMEOUT);
                }
                System.out.println("Sent MQTT DISCONNECT packet was acknowledged");
            }

            //Codes_SRS_Mqtt_25_009: [The function shall close the MQTT client.]
            this.mqttConnection.close();
            this.mqttConnection.setMqttAsyncClient(null);
        }
        catch (MqttException e)
        {
            System.out.println("Exception encountered while sending MQTT DISCONNECT packet" + e);

            //Codes_SRS_Mqtt_25_011: [If an MQTT connection is unable to be closed for any reason, the function shall throw a TransportException.]
            throw PahoExceptionTranslator.convertToMqttException(e, "Unable to disconnect");
        }
    }

    /**
     * Method to publish to mqtt broker connection.
     *
     * @param publishTopic the topic to publish on mqtt broker connection.
     * @param message the message to publish.
     * @throws TransportException if sas token has expired, if connection hasn't been established yet, or if Paho throws
     * for any other reason
     */
    protected void publish(String publishTopic, Message message) throws TransportException
    {
        try
        {
            if (this.mqttConnection.getMqttAsyncClient() == null)
            {
                TransportException transportException = new TransportException("Need to open first!");
                transportException.setRetryable(true);
                throw transportException;
            }

            if (!this.mqttConnection.getMqttAsyncClient().isConnected())
            {
                //Codes_SRS_Mqtt_25_012: [If the MQTT connection is closed, the function shall throw a TransportException.]
                TransportException transportException = new TransportException("Cannot publish when mqtt client is disconnected");
                transportException.setRetryable(true);
                throw transportException;
            }

            if (message == null || publishTopic == null || publishTopic.length() == 0 || message.getBytes() == null)
            {
                //Codes_SRS_Mqtt_25_013: [If the either publishTopic is null or empty or if payload is null, the function shall throw an IllegalArgumentException.]
                throw new IllegalArgumentException("Cannot publish on null or empty publish topic");
            }

            byte[] payload = message.getBytes();

            while (this.mqttConnection.getMqttAsyncClient().getPendingDeliveryTokens().length >= MqttConnection.MAX_IN_FLIGHT_COUNT)
            {
                //Codes_SRS_Mqtt_25_048: [publish shall check for pending publish tokens by calling getPendingDeliveryTokens. And if there are pending tokens publish shall sleep until the number of pending tokens are less than 10 as per paho limitations]
                Thread.sleep(10);

                if (this.mqttConnection.getMqttAsyncClient() == null)
                {
                    TransportException transportException = new TransportException("Connection was lost while waiting for mqtt deliveries to finish");
                    transportException.setRetryable(true);
                    throw transportException;
                }

                if (!this.mqttConnection.getMqttAsyncClient().isConnected())
                {
                    //Codes_SRS_Mqtt_25_012: [If the MQTT connection is closed, the function shall throw a ProtocolException.]
                    TransportException transportException = new TransportException("Cannot publish when mqtt client is holding 10 tokens and is disconnected");
                    transportException.setRetryable(true);
                    throw transportException;
                }
            }

            MqttMessage mqttMessage = (payload.length == 0) ? new MqttMessage() : new MqttMessage(payload);

            mqttMessage.setQos(MqttConnection.QOS);

            synchronized (this.publishLock)
            {
                System.out.println("Publishing message (" + message + ") to MQTT topic " + publishTopic);
                //Codes_SRS_Mqtt_25_014: [The function shall publish message payload on the publishTopic specified to the IoT Hub given in the configuration.]
                IMqttDeliveryToken publishToken = this.mqttConnection.getMqttAsyncClient().publish(publishTopic, mqttMessage);
                unacknowledgedSentMessages.put(publishToken.getMessageId(), message);
                System.out.println("Message published to MQTT topic " + publishTopic + ". Mqtt message id " + publishToken.getMessageId() + " added to list of messages to wait for acknowledgement: " + message);
            }
        }
        catch (MqttException e)
        {
            System.out.println("Message could not be published to MQTT topic " + publishTopic + " msg: " + message + " ex: " + e);

            //Codes_SRS_Mqtt_25_047: [If the Mqtt Client Async throws MqttException, the function shall throw a ProtocolException with the message.]
            throw PahoExceptionTranslator.convertToMqttException(e, "Unable to publish message on topic : " + publishTopic);
        }
        catch (InterruptedException e)
        {
            throw new TransportException("Interrupted, Unable to publish message on topic : " + publishTopic, e);
        }
    }

    /**
     * Method to subscribe to mqtt broker connection.
     *
     * @param topic the topic to subscribe on mqtt broker connection.
     * @throws TransportException if failed to subscribe the mqtt topic.
     * @throws IllegalArgumentException if topic is null
     */
    protected void subscribe(String topic) throws TransportException
    {
        synchronized (this.stateLock)
        {
            try
            {
                if (topic == null)
                {
                    //Codes_SRS_Mqtt_25_016: [If the subscribeTopic is null or empty, the function shall throw an IllegalArgumentException.]
                    throw new IllegalArgumentException("Topic cannot be null");

                }
                else if (!this.mqttConnection.getMqttAsyncClient().isConnected())
                {

                    //Codes_SRS_Mqtt_25_015: [If the MQTT connection is closed, the function shall throw a TransportException with message.]
                    TransportException transportException = new TransportException("Cannot subscribe when mqtt client is disconnected");
                    transportException.setRetryable(true);
                    throw transportException;
                }

                System.out.println("Sending MQTT SUBSCRIBE packet for topic " + topic);

                //Codes_SRS_Mqtt_25_017: [The function shall subscribe to subscribeTopic specified to the IoT Hub given in the configuration.]
                IMqttToken subToken = this.mqttConnection.getMqttAsyncClient().subscribe(topic, MqttConnection.QOS);

                subToken.waitForCompletion(MqttConnection.MAX_SUBSCRIBE_ACK_WAIT_TIME);
                System.out.println("Sent MQTT SUBSCRIBE packet for topic " + topic + " was acknowledged");

            }
            catch (MqttException e)
            {
                System.out.println("Encountered exception while sending MQTT SUBSCRIBE packet for topic " + topic + e);

                //Codes_SRS_Mqtt_25_048: [If the Mqtt Client Async throws MqttException for any reason, the function shall throw a ProtocolException with the message.]
                throw PahoExceptionTranslator.convertToMqttException(e, "Unable to subscribe to topic :" + topic);
            }
        }
    }

    /**
     * Method to receive messages on mqtt broker connection.
     *
     * @return a received message. It can be {@code null}
     * @throws TransportException if failed to receive mqtt message.
     */
    public IotHubTransportMessage receive() throws TransportException
    {
        synchronized (this.incomingLock)
        {
            if (this.mqttConnection == null)
            {
                throw new TransportException(new IllegalArgumentException("Mqtt client should be initialised at least once before using it"));
            }

            // Codes_SRS_Mqtt_34_023: [This method shall call peekMessage to get the message payload from the received Messages queue corresponding to the messaging client's operation.]
            Pair<String, byte[]> messagePair = peekMessage();
            if (messagePair != null)
            {
                String topic = messagePair.getKey();
                if (topic != null)
                {
                    byte[] data = messagePair.getValue();
                    if (data != null)
                    {
                        //remove this message from the queue as this is the correct handler
                        allReceivedMessages.poll();

                        // Codes_SRS_Mqtt_34_024: [This method shall construct new Message with the bytes obtained from peekMessage and return the message.]
                        return new IotHubTransportMessage(data, topic);
                    }
                    else
                    {
                        // Codes_SRS_Mqtt_34_025: [If the call to peekMessage returns null when topic is non-null then this method will throw a TransportException]
                        throw new TransportException("Data cannot be null when topic is non-null");
                    }
                }
                else
                {
                    // Codes_SRS_Mqtt_34_022: [If the call peekMessage returns a null or empty string then this method shall do nothing and return null]
                    return null;
                }
            }

            // Codes_SRS_Mqtt_34_021: [If the call peekMessage returns null then this method shall do nothing and return null]
            return null;
        }
    }

    /**
     * Event fired when the connection with the MQTT broker is lost.
     * @param throwable Reason for losing the connection.
     */
    @Override
    public void connectionLost(Throwable throwable)
    {
        TransportException ex = null;

        System.out.println("Mqtt connection lost" + throwable);

        try
        {
            if (mqttConnection != null)
            {
                this.disconnect();
            }
        }
        catch (TransportException e)
        {
            ex = e;
        }

        if (this.listener != null)
        {
            if (ex == null)
            {
                if (throwable instanceof MqttException)
                {
                    //Codes_SRS_Mqtt_34_055: [If the provided throwable is an instance of MqttException, this function shall derive the associated ConnectionStatusException and notify the listener of that derived exception.]
                    throwable = PahoExceptionTranslator.convertToMqttException((MqttException) throwable, "Mqtt connection lost");
                    System.out.println("Mqtt connection loss interpreted into transport exception" + throwable);
                }
                else
                {
                    throwable = new TransportException(throwable);
                }
            }
            else
            {
                throwable = ex;
            }

            //Codes_SRS_Mqtt_34_045: [If this object has a saved listener, this function shall notify the listener that connection was lost.]
            ReconnectionNotifier.notifyDisconnectAsync(throwable, this.listener, this.connectionId);
        }
    }

    /**
     * Event fired when the message arrived on the MQTT broker.
     * @param topic the topic on which message arrived.
     * @param mqttMessage  the message arrived on the Mqtt broker.
     */
    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage)
    {
        System.out.println("Mqtt message arrived on topic "+  topic + " with mqtt message id: " +  mqttMessage.getId());
        //Codes_SRS_Mqtt_25_030: [The payload of the message and the topic is added to the received messages queue .]
        this.mqttConnection.getAllReceivedMessages().add(new MutablePair<>(topic, mqttMessage.getPayload()));

        if (this.messageListener != null)
        {
            //Codes_SRS_Mqtt_34_045: [If there is a saved listener, this function shall notify that listener that a message arrived.]
            this.messageListener.onMessageArrived(mqttMessage.getId());
        }
    }

    /**
     * Event fired when the message arrived on the MQTT broker.
     * @param iMqttDeliveryToken the MqttDeliveryToken for which the message was successfully sent.
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken)
    {
        Message deliveredMessage = null;
        System.out.println("Mqtt message with message id " + iMqttDeliveryToken.getMessageId() + " was acknowledge by service");
        synchronized (this.publishLock)
        {
            if (this.listener != null && unacknowledgedSentMessages.containsKey(iMqttDeliveryToken.getMessageId()))
            {
                System.out.println("Mqtt message with message id " + iMqttDeliveryToken.getMessageId() + " that was acknowledge by service was sent by this client");
                deliveredMessage = unacknowledgedSentMessages.remove(iMqttDeliveryToken.getMessageId());
            }
            else
            {
                System.out.println("Mqtt message with message id " + iMqttDeliveryToken.getMessageId() + " that was acknowledge by service was not sent by this client, will be ignored");
            }
        }

        //Codes_SRS_Mqtt_34_042: [If this object has a saved listener, that listener shall be notified of the successfully delivered message.]
        this.listener.onMessageSent(deliveredMessage, null);
    }

    public Pair<String, byte[]> peekMessage()
    {
        return this.allReceivedMessages.peek();
    }

    /**
     * Attempts to send ack for the provided message. If the message does not have a saved messageId in this layer,
     * this function shall return false.
     * @param messageId The message id to send the ack for
     * @return true if the ack is sent successfully or false if the message isn't tied to this mqtt client
     * @throws TransportException if an exception occurs when sending the ack
     */
    protected boolean sendMessageAcknowledgement(int messageId) throws TransportException
    {
        //Codes_SRS_Mqtt_34_043: [This function shall invoke the saved mqttConnection object to send the message acknowledgement for the provided messageId and return that result.]
        System.out.println("Sending mqtt ack for received message with mqtt message id " + messageId);
        return this.mqttConnection.sendMessageAcknowledgement(messageId);
    }

}
