package com.microsoft.azure.sdk.iot.device.transport;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.exceptions.TransportException;
import com.microsoft.azure.sdk.iot.device.transport.*;
import com.microsoft.azure.sdk.iot.device.transport.mqtt.MqttIotHubConnection;
import com.microsoft.azure.sdk.iot.device.transport.mqtt.exceptions.MqttUnauthorizedException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.MqttException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class IotHubTransport  implements IotHubListener
{
    private static final int MAX_MESSAGES_TO_SEND_PER_THREAD = 10;
    private MqttIotHubConnection iotHubTransportConnection;
    //Lock on reading and writing on the inProgressPackets map
    final private Object inProgressMessagesLock = new Object();
    /* Messages waiting to be sent to the IoT Hub. */
    private final Queue<IotHubTransportPacket> waitingPacketsQueue = new ConcurrentLinkedQueue<>();
    /* Messages which are sent to the IoT Hub but did not receive ack yet. */
    private final Map<String, IotHubTransportPacket> inProgressPackets = new ConcurrentHashMap<>();
    /* Messages received from the IoT Hub */
    private final Queue<IotHubTransportMessage> receivedMessagesQueue = new ConcurrentLinkedQueue<>();
    final private Object reconnectionLock = new Object();
    private volatile IotHubConnectionStatus connectionStatus;
    private long receivePeriodInMilliseconds;
    private int currentReconnectionAttempt;
    private long reconnectionAttemptStartTimeMillis;

    /*Connection Status change callback information */
    private IotHubConnectionStatusChangeCallback connectionStatusChangeCallback;
    private Object connectionStatusChangeCallbackContext;


    private ScheduledExecutorService taskScheduler;

    private ScheduledExecutorService scheduledExecutorService;
    private static final int POOL_SIZE = 1;
    private DeviceClientConfig config;

    public IotHubTransport(DeviceClientConfig config) throws IllegalArgumentException
    {
        this.connectionStatus = IotHubConnectionStatus.DISCONNECTED;
        this.currentReconnectionAttempt = 0;
        this.receivePeriodInMilliseconds = 500;
        this.config = config;
    }
    
    @Override
    public void onMessageSent(Message message, Throwable e)
    {
        if (message == null)
        {
            System.out.println("onMessageSent called with null message");
            return;
        }

        System.out.println("IotHub message was acknowledged. Checking if there is record of sending this message: " + message);

        // remove from in progress queue and add to callback queue
        IotHubTransportPacket packet = null;
        synchronized (this.inProgressMessagesLock)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_004: [This function shall retrieve a packet from the inProgressPackets
            // queue with the message id from the provided message if there is one.]
            packet = inProgressPackets.remove(message.getMessageId());
        }

        if (packet != null)
        {
            if (e == null)
            {
                //Codes_SRS_IOTHUBTRANSPORT_34_005: [If there was a packet in the inProgressPackets queue tied to the
                // provided message, and the provided throwable is null, this function shall set the status of that
                // packet to OK_EMPTY and add it to the callbacks queue.]
                System.out.println("Message was sent by this client, adding it to callbacks queue with OK_EMPTY ({})" + message);
                packet.setStatus(IotHubStatusCode.OK_EMPTY);
            }
            else
            {
                if (e instanceof TransportException)
                {
                    //Codes_SRS_IOTHUBTRANSPORT_34_006: [If there was a packet in the inProgressPackets queue tied to
                    // the provided message, and the provided throwable is a TransportException, this function shall
                    // call "handleMessageException" with the provided packet and transport exception.]
                    this.handleMessageException(packet, (TransportException) e);
                }
                else
                {
                    //Codes_SRS_IOTHUBTRANSPORT_34_007: [If there was a packet in the inProgressPackets queue tied to
                    // the provided message, and the provided throwable is not a TransportException, this function
                    // shall call "handleMessageException" with the provided packet and a new transport exception with
                    // the provided exception as the inner exception.]
                    this.handleMessageException(packet, new TransportException(e));
                }
            }
        }
        else if (message != null)
        {
            System.out.println("A message was acknowledged by IoT Hub, but this client has no record of sending it ({})" + message);
        }
    }

    @Override
    public void onMessageReceived(IotHubTransportMessage message, Throwable e)
    {
        if (message != null && e != null)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_008: [If this function is called with a non-null message and a non-null
            // throwable, this function shall log an IllegalArgumentException.]
            System.out.println("Exception encountered while receiving a message from service {}" + message + e);
        }
        else if (message != null)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_009: [If this function is called with a non-null message and a null
            // exception, this function shall add that message to the receivedMessagesQueue.]
            System.out.println("Message was received from IotHub ({})" + message);
            this.receivedMessagesQueue.add(message);
        }
        else
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_010: [If this function is called with a null message and a non-null
            // throwable, this function shall log that exception.]
            System.out.println("Exception encountered while receiving messages from service" + e);
        }
    }

    @Override
    public void onConnectionLost(Throwable e, String connectionId)
    {
        synchronized (this.reconnectionLock)
        {
            if (!connectionId.equals(this.iotHubTransportConnection.getConnectionId()))
            {
                //Codes_SRS_IOTHUBTRANSPORT_34_078: [If this function is called with a connection id that is not the same
                // as the current connection id, this function shall do nothing.]

                //This connection status update is for a connection that is no longer tracked at this level, so it can be ignored.
                System.out.println("OnConnectionLost was fired, but for an outdated connection. Ignoring...");
                return;
            }

            if (this.connectionStatus != IotHubConnectionStatus.CONNECTED)
            {
                //Codes_SRS_IOTHUBTRANSPORT_34_011: [If this function is called while the connection status is DISCONNECTED,
                // this function shall do nothing.]
                System.out.println("OnConnectionLost was fired, but connection is already disocnnected. Ignoring...");
                return;
            }

            if (e instanceof TransportException)
            {
                //Codes_SRS_IOTHUBTRANSPORT_34_012: [If this function is called with a TransportException, this function
                // shall call handleDisconnection with that exception.]
                this.handleDisconnection((TransportException) e);
            }
            else
            {
                //Codes_SRS_IOTHUBTRANSPORT_34_013: [If this function is called with any other type of exception, this
                // function shall call handleDisconnection with that exception as the inner exception in a new
                // TransportException.]
                this.handleDisconnection(new TransportException(e));
            }
        }
    }

    @Override
    public void onConnectionEstablished(String connectionId)
    {
        if (connectionId.equals(this.iotHubTransportConnection.getConnectionId()))
        {
            System.out.println("The connection to the IoT Hub has been established");

            //Codes_SRS_IOTHUBTRANSPORT_34_014: [If the provided connectionId is associated with the current connection, This function shall invoke updateStatus with status CONNECTED, change
            // reason CONNECTION_OK and a null throwable.]
            this.updateStatus(IotHubConnectionStatus.CONNECTED, IotHubConnectionStatusChangeReason.CONNECTION_OK, null);
        }
    }

    /**
     * Establishes a communication channel with an IoT Hub. If a channel is
     * already open, the function shall do nothing.
     *
     * If reconnection is occurring when this is called, this function shall block and wait for the reconnection
     * to finish before trying to open the connection
     *
     *
     * @throws TransportException if a communication channel cannot be
     * established.
     */
    public void open() throws TransportException
    {
        if (this.connectionStatus == IotHubConnectionStatus.CONNECTED)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_017: [If the connection status of this object is CONNECTED, this function
            // shall do nothing.]
            return;
        }

        if (this.connectionStatus == IotHubConnectionStatus.DISCONNECTED_RETRYING)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_016: [If the connection status of this object is DISCONNECTED_RETRYING, this
            // function shall throw a TransportException.]
            throw new TransportException("Open cannot be called while transport is reconnecting");
        }

        this.taskScheduler = Executors.newScheduledThreadPool(1);

        //Codes_SRS_IOTHUBTRANSPORT_34_019: [This function shall open the invoke the method openConnection.]
        openConnection();

        System.out.println("Client connection opened successfully");
    }

    /**
     * Registers a callback to be executed whenever the connection status to the IoT Hub has changed.
     *
     * @param callback the callback to be called. Can be null if callbackContext is not null
     * @param callbackContext a context to be passed to the callback. Can be {@code null}.
     */
    public void registerConnectionStatusChangeCallback(IotHubConnectionStatusChangeCallback callback, Object callbackContext)
    {
        if (callbackContext != null && callback == null)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_051: [If the provided callback is null but the context is not, this function shall throw an IllegalArgumentException.]
            throw new IllegalArgumentException("Callback cannot be null if callback context is null");
        }

        //Codes_SRS_IOTHUBTRANSPORT_34_052: [This function shall save the provided callback and context.]
        this.connectionStatusChangeCallback = callback;
        this.connectionStatusChangeCallbackContext = callbackContext;
    }

    /**
     * Moves all packets from waiting queue and in progress map into callbacks queue with status MESSAGE_CANCELLED_ONCLOSE
     */
    private void cancelPendingPackets()
    {
        //Codes_SRS_IOTHUBTRANSPORT_34_021: [This function shall move all waiting messages to the callback queue with
        // status MESSAGE_CANCELLED_ONCLOSE.]
        IotHubTransportPacket packet = this.waitingPacketsQueue.poll();
        while (packet != null)
        {
            packet.setStatus(IotHubStatusCode.MESSAGE_CANCELLED_ONCLOSE);

            packet = this.waitingPacketsQueue.poll();
        }

        synchronized (this.inProgressMessagesLock)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_022: [This function shall move all in progress messages to the callback queue
            // with status MESSAGE_CANCELLED_ONCLOSE.]
            for (Map.Entry<String, IotHubTransportPacket> packetEntry : inProgressPackets.entrySet())
            {
                IotHubTransportPacket inProgressPacket = packetEntry.getValue();
                inProgressPacket.setStatus(IotHubStatusCode.MESSAGE_CANCELLED_ONCLOSE);
            }

            inProgressPackets.clear();
        }
    }

    /**
     * If the provided received message has a saved callback, this function shall execute that callback and send the ack
     * to the service
     * @param receivedMessage the message to acknowledge
     * @throws TransportException if any exception is encountered while sending the acknowledgement
     */
    private void acknowledgeReceivedMessage(IotHubTransportMessage receivedMessage) throws TransportException
    {
        MessageCallback messageCallback = receivedMessage.getMessageCallback();
        Object messageCallbackContext = receivedMessage.getMessageCallbackContext();

        if (messageCallback != null)
        {
            System.out.println("Executing callback for received message: " + receivedMessage);
            //Codes_SRS_IOTHUBTRANSPORT_34_053: [This function shall execute the callback associate with the provided
            // transport message with the provided message and its saved callback context.]
            IotHubMessageResult result = messageCallback.execute(receivedMessage, messageCallbackContext);

            try
            {
                //Codes_SRS_IOTHUBTRANSPORT_34_054: [This function shall send the message callback result along the
                // connection as the ack to the service.]
                System.out.println("Sending acknowledgement for received cloud to device message: " + receivedMessage);
                this.iotHubTransportConnection.sendMessageResult(receivedMessage);
            }
            catch (TransportException e)
            {
                //Codes_SRS_IOTHUBTRANSPORT_34_055: [If an exception is thrown while acknowledging the received message,
                // this function shall add the received message back into the receivedMessagesQueue and then rethrow the exception.]
                System.out.println("Sending acknowledgement for received cloud to device message failed, adding it back to the queue: " + receivedMessage);
                this.receivedMessagesQueue.add(receivedMessage);
                throw e;
            }
        }
    }

    /**
     * Closes all resources used to communicate with an IoT Hub. Once {@code close()} is
     * called, the transport is no longer usable. If the transport is already
     * closed, the function shall do nothing.
     *
     * @param cause the cause of why this connection is closing, to be reported over connection status change callback
     * @param reason the reason to close this connection, to be reported over connection status change callback
     *
     * @throws TransportException if an error occurs in closing the transport.
     */
    public void close(IotHubConnectionStatusChangeReason reason, Throwable cause) throws TransportException
    {
        if (reason == null)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_026: [If the supplied reason is null, this function shall throw an
            // IllegalArgumentException.]
            throw new IllegalArgumentException("reason cannot be null");
        }

        this.cancelPendingPackets();

        if (this.taskScheduler != null)
        {
            this.taskScheduler.shutdown();
        }

        if (this.scheduledExecutorService != null)
        {
            this.scheduledExecutorService.shutdownNow();
            this.scheduledExecutorService = null;
        }

        //Codes_SRS_IOTHUBTRANSPORT_34_024: [This function shall close the connection.]
        if (this.iotHubTransportConnection != null)
        {
            this.iotHubTransportConnection.close();
        }

        //Codes_SRS_IOTHUBTRANSPORT_34_025: [This function shall invoke updateStatus with status DISCONNECTED and the
        // supplied reason and cause.]
        this.updateStatus(IotHubConnectionStatus.DISCONNECTED, reason, cause);

        System.out.println("Client connection closed successfully");
    }


    /**
     * Adds a message to the transport queue.
     *
     * @param message the message to be sent.
     */
    public void addMessage(Message message)
    {
        if (this.connectionStatus == IotHubConnectionStatus.DISCONNECTED)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_041: [If this object's connection state is DISCONNECTED, this function shall
            // throw an IllegalStateException.]
            throw new IllegalStateException("Cannot add a message when the transport is closed.");
        }

        //Codes_SRS_IOTHUBTRANSPORT_34_042: [This function shall build a transport packet from the provided message,
        // callback, and context and then add that packet to the waiting queue.]
        IotHubTransportPacket packet = new IotHubTransportPacket(message, null, System.currentTimeMillis());
        this.waitingPacketsQueue.add(packet);
        System.out.println("Message was queued to be sent later: " + message);
    }

    /**
     * Sends all messages on the transport queue. If a previous send attempt had
     * failed, the function will attempt to resend the messages in the previous
     * attempt.
     */
    public void sendMessages()
    {
        if (this.connectionStatus == IotHubConnectionStatus.DISCONNECTED
                || this.connectionStatus == IotHubConnectionStatus.DISCONNECTED_RETRYING)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_043: [If the connection status of this object is not CONNECTED, this function shall do nothing]
            return;
        }

        int timeSlice = MAX_MESSAGES_TO_SEND_PER_THREAD;

        while (this.connectionStatus == IotHubConnectionStatus.CONNECTED && timeSlice-- > 0)
        {
            IotHubTransportPacket packet = waitingPacketsQueue.poll();
            if (packet != null)
            {
                Message message = packet.getMessage();
                System.out.println("Dequeued a message from waiting queue to be sent: " + message);

                if (message != null)
                {
                    //Codes_SRS_IOTHUBTRANSPORT_34_044: [This function continue to dequeue packets saved in the waiting
                    // queue and send them until connection status isn't CONNECTED or until 10 messages have been sent]
                    sendPacket(packet);
                }
            }
        }
    }

    /**
     * <p>
     * Invokes the message callback if a message is found and
     * responds to the IoT Hub on how the processed message should be
     * handled by the IoT Hub.
     * </p>
     * If no message callback is set, the function will do nothing.
     *
     * @throws TransportException if the server could not be reached.
     */
    public void handleMessage() throws TransportException
    {
        //Codes_SRS_IOTHUBTRANSPORT_34_046: [If this object's connection status is not CONNECTED, this function shall do nothing.]
        if (this.connectionStatus == IotHubConnectionStatus.CONNECTED)
        {
            IotHubTransportMessage receivedMessage = this.receivedMessagesQueue.poll();
            if (receivedMessage != null)
            {
                //Codes_SRS_IOTHUBTRANSPORT_34_048: [If this object's connection status is CONNECTED and there is a
                // received message in the queue, this function shall acknowledge the received message
                this.acknowledgeReceivedMessage(receivedMessage);
            }
        }
    }

    /**
     * Spawn a task to add the provided packet back to the waiting list if the provided transportException is retryable
     * and if the message hasn't timed out
     * @param packet the packet to retry
     * @param transportException the exception encountered while sending this packet before
     */
    private void handleMessageException(IotHubTransportPacket packet, TransportException transportException)
    {
        System.out.println("Handling an exception from sending message: Attempt number {}" + packet.getCurrentRetryAttempt() + transportException);
        // todo...
    }

    /**
     * Maps a given throwable to an IotHubConnectionStatusChangeReason
     * @param e the throwable to map to an IotHubConnectionStatusChangeReason
     * @return the mapped IotHubConnectionStatusChangeReason
     */
    private IotHubConnectionStatusChangeReason exceptionToStatusChangeReason(Throwable e)
    {
        if (e instanceof TransportException)
        {
            TransportException transportException = (TransportException) e;
            if (transportException.isRetryable())
            {
                System.out.println("Mapping throwable to NO_NETWORK because it was a retryable exception: " + e);

                //Codes_SRS_IOTHUBTRANSPORT_34_033: [If the provided exception is a retryable TransportException,
                // this function shall return NO_NETWORK.]
                return IotHubConnectionStatusChangeReason.NO_NETWORK;
            } else if (e instanceof MqttUnauthorizedException)
            {
                System.out.println("Mapping throwable to BAD_CREDENTIAL because it was a non-retryable exception authorization exception but the saved sas token has not expired yet: " + e);

                //Codes_SRS_IOTHUBTRANSPORT_34_035: [If the provided exception is a TransportException that isn't
                // retryable and the saved sas token has not expired, but the exception is an unauthorized exception,
                // this function shall return BAD_CREDENTIAL.]
                return IotHubConnectionStatusChangeReason.BAD_CREDENTIAL;
            }
        }

        System.out.println("Mapping exception throwable to COMMUNICATION_ERROR because the sdk was unable to classify the thrown exception to anything other category: " + e);

        //Codes_SRS_IOTHUBTRANSPORT_34_032: [If the provided exception is not a TransportException, this function shall
        // return COMMUNICATION_ERROR.]
        return IotHubConnectionStatusChangeReason.COMMUNICATION_ERROR;
    }


    /**
     * Creates a new iotHubTransportConnection instance, sets this object as its listener, and opens that connection
     * @throws TransportException if any exception is thrown while opening the connection
     */
    private void openConnection() throws TransportException
    {
        scheduledExecutorService = Executors.newScheduledThreadPool(POOL_SIZE);

        if (this.iotHubTransportConnection == null)
        {
            this.iotHubTransportConnection = new MqttIotHubConnection(this.config);
        }

        //Codes_SRS_IOTHUBTRANSPORT_34_038: [This function shall set this object as the listener of the iotHubTransportConnection object.]
        this.iotHubTransportConnection.setListener(this);

        //Codes_SRS_IOTHUBTRANSPORT_34_039: [This function shall open the iotHubTransportConnection object with the saved list of configs.]
        this.iotHubTransportConnection.open(null, scheduledExecutorService);

        //Codes_SRS_IOTHUBTRANSPORT_34_040: [This function shall invoke the method updateStatus with status CONNECTED,
        // reason CONNECTION_OK, and a null throwable.]
        this.updateStatus(IotHubConnectionStatus.CONNECTED, IotHubConnectionStatusChangeReason.CONNECTION_OK, null);
    }

    /**
     * Attempts to reconnect. By the end of this call, the state of this object shall be either CONNECTED or DISCONNECTED
     * @param transportException the exception that caused the disconnection
     */
    private void handleDisconnection(TransportException transportException)
    {
        System.out.println("Handling a disconnection event: " + transportException);

        synchronized (this.inProgressMessagesLock)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_057: [This function shall move all packets from inProgressQueue to waiting queue.]
            System.out.println("Due to disconnection event, clearing active queues, and re-queueing them to waiting queues to be re-processed later upon reconnection");
            this.waitingPacketsQueue.addAll(inProgressPackets.values());
            inProgressPackets.clear();
        }

        //Codes_SRS_IOTHUBTRANSPORT_34_058: [This function shall invoke updateStatus with DISCONNECTED_RETRYING, and the provided transportException.]
        this.updateStatus(IotHubConnectionStatus.DISCONNECTED_RETRYING, exceptionToStatusChangeReason(transportException), transportException);

        System.out.println("Starting reconnection logic");
        //Codes_SRS_IOTHUBTRANSPORT_34_060: [This function shall invoke reconnect with the provided exception.]
        reconnect(transportException);
    }

    /**
     * Attempts to close and then re-open the connection until connection reestablished, retry policy expires, or a
     * terminal exception is encountered. At the end of this call, the state of this object should be either
     * CONNECTED or DISCONNECTED depending on how reconnection goes.
     */
    private void reconnect(TransportException transportException)
    {
        if (this.reconnectionAttemptStartTimeMillis == 0)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_065: [If the saved reconnection attempt start time is 0, this function shall
            // save the current time as the time that reconnection started.]
            this.reconnectionAttemptStartTimeMillis = System.currentTimeMillis();
        }

        RetryDecision retryDecision = null;

        //Codes_SRS_IOTHUBTRANSPORT_34_066: [This function shall attempt to reconnect while this object's state is
        // DISCONNECTED_RETRYING, the operation hasn't timed out, and the last transport exception is retryable.]
        while (this.connectionStatus == IotHubConnectionStatus.DISCONNECTED_RETRYING
                && transportException != null
                && transportException.isRetryable())
        {
            System.out.println("Attempting reconnect attempt: " + this.currentReconnectionAttempt);
            this.currentReconnectionAttempt++;

            RetryPolicy retryPolicy = this.config.getRetryPolicy();
            retryDecision = retryPolicy.getRetryDecision(this.currentReconnectionAttempt, transportException);
            if (!retryDecision.shouldRetry())
            {
                break;
            }

            System.out.println("Sleeping between reconnect attempts");
            //Want to sleep without interruption because the only interruptions expected are threads that add a message
            // to the waiting list again. Those threads should wait until after reconnection finishes first because
            // they will constantly fail until connection is re-established
            IotHubTransport.sleepUninterruptibly(retryDecision.getDuration(), MILLISECONDS);


            transportException = singleReconnectAttempt();
        }

        // reconnection may have failed, so check last retry decision, check for timeout, and check if last exception
        // was terminal
        try
        {
            if (retryDecision != null && !retryDecision.shouldRetry())
            {
                //Codes_SRS_IOTHUBTRANSPORT_34_068: [If the reconnection effort ends because the retry policy said to
                // stop, this function shall invoke close with RETRY_EXPIRED and the last transportException.]
                System.out.println("Reconnection was abandoned due to the retry policy");
                this.close(IotHubConnectionStatusChangeReason.RETRY_EXPIRED, transportException);
            }
            else if (transportException != null && !transportException.isRetryable())
            {
                //Codes_SRS_IOTHUBTRANSPORT_34_070: [If the reconnection effort ends because a terminal exception is
                // encountered, this function shall invoke close with that terminal exception.]
                System.out.println("Reconnection was abandoned due to encountering a non-retryable exception: " + transportException);
                this.close(this.exceptionToStatusChangeReason(transportException), transportException);
            }
        }
        catch (TransportException ex)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_071: [If an exception is encountered while closing, this function shall invoke
            // updateStatus with DISCONNECTED, COMMUNICATION_ERROR, and the last transport exception.]
            System.out.println("Encountered an exception while closing the client object, client instance should no longer be used as the state is unknown: " + ex);
            this.updateStatus(IotHubConnectionStatus.DISCONNECTED, IotHubConnectionStatusChangeReason.COMMUNICATION_ERROR, transportException);
        }
    }

    /**
     * Attempts to close and then re-open the iotHubTransportConnection once
     * @return the exception encountered during closing or opening, or null if reconnection succeeded
     */
    private TransportException singleReconnectAttempt()
    {
        try
        {
            System.out.println("Attempting to close and re-open the iot hub transport connection...");
            //Codes_SRS_IOTHUBTRANSPORT_34_061: [This function shall close the saved connection, and then invoke openConnection and return null.]
            this.iotHubTransportConnection.close();
            this.openConnection();
            System.out.println("Successfully closed and re-opened the iot hub transport connection");
        }
        catch (TransportException newTransportException)
        {
            //Codes_SRS_IOTHUBTRANSPORT_34_062: [If an exception is encountered while closing or opening the connection,
            // this function shall invoke checkForUnauthorizedException on that exception and then return it.]
            System.out.println("Failed to close and re-open the iot hub transport connection, checking if another retry attempt should be made: " + newTransportException);
            return newTransportException;
        }

        return null;
    }

    /**
     * Sends a single packet over the iotHubTransportConnection and handles the response
     * @param packet the packet to send
     */
    private void sendPacket(IotHubTransportPacket packet)
    {
        Message message = packet.getMessage();

        try
        {
            synchronized (this.inProgressMessagesLock)
            {
                System.out.println("Adding transport message to the inProgressPackets to wait for acknowledgement: " + message);
                this.inProgressPackets.put(message.getMessageId(), packet);
            }

            //Codes_SRS_IOTHUBTRANSPORT_34_073: [This function shall send the provided message over the saved connection
            // and save the response code.]
            System.out.println("Sending message: " + message);
            IotHubStatusCode statusCode = this.iotHubTransportConnection.sendMessage(message);
            System.out.println("Sent message (" + message + ") to protocol level, returned status code was: " + statusCode);

            if (statusCode != IotHubStatusCode.OK_EMPTY && statusCode != IotHubStatusCode.OK)
            {
                //Codes_SRS_IOTHUBTRANSPORT_34_074: [If the response from sending is not OK or OK_EMPTY, this function
                // shall invoke handleMessageException with that message.]
                this.handleMessageException(this.inProgressPackets.remove(message.getMessageId()), IotHubStatusCode.getConnectionStatusException(statusCode, ""));
            }
        }
        catch (TransportException transportException)
        {
            System.out.println("Encountered exception while sending message with correlation id: ");
            IotHubTransportPacket outboundPacket;

            synchronized (this.inProgressMessagesLock)
            {
                outboundPacket = this.inProgressPackets.remove(message.getMessageId());
            }

            //Codes_SRS_IOTHUBTRANSPORT_34_076: [If an exception is encountered while sending the message, this function
            // shall invoke handleMessageException with that packet.]
            this.handleMessageException(outboundPacket, transportException);
        }
    }


    /**
     * If the provided newConnectionStatus is different from the current connection status, this function shall update
     * the saved connection status and notify all callbacks listening for connection state changes
     * @param newConnectionStatus the new connection status
     * @param reason the reason for the new connection status
     * @param throwable the associated exception to the connection status change
     */
    private void updateStatus(IotHubConnectionStatus newConnectionStatus, IotHubConnectionStatusChangeReason reason, Throwable throwable)
    {
        //Codes_SRS_IOTHUBTRANSPORT_28_005:[This function shall updated the saved connection status if the connection status has changed.]
        //Codes_SRS_IOTHUBTRANSPORT_28_006:[This function shall invoke all callbacks listening for the state change if the connection status has changed.]
        if (this.connectionStatus != newConnectionStatus)
        {
            if (throwable == null)
            {
                System.out.println("Updating transport status to new status " + newConnectionStatus + " with reason: " + reason);
            }
            else
            {
                System.out.println("Updating transport status to new status " + newConnectionStatus + " with reason: " + reason);
            }

            this.connectionStatus = newConnectionStatus;

            //invoke connection status callbacks
            System.out.println("Invoking connection status callbacks with new status details");
            invokeConnectionStatusChangeCallback(newConnectionStatus, reason, throwable);

            if (newConnectionStatus == IotHubConnectionStatus.CONNECTED)
            {
                //Codes_SRS_IOTHUBTRANSPORT_28_007: [This function shall reset currentReconnectionAttempt and reconnectionAttemptStartTimeMillis if connection status is changed to CONNECTED.]
                this.currentReconnectionAttempt = 0;
                this.reconnectionAttemptStartTimeMillis = 0;
            }
        }
    }
    /**
     * Notify the connection status change callback
     * @param status the status to notify of
     * @param reason the reason for that status
     * @param e the associated exception. May be null
     */
    private void invokeConnectionStatusChangeCallback(IotHubConnectionStatus status, IotHubConnectionStatusChangeReason reason, Throwable e)
    {
        //Codes_SRS_IOTHUBTRANSPORT_28_004:[This function shall notify the connection status change callback if the callback is not null]
        if (this.connectionStatusChangeCallback != null)
        {
            this.connectionStatusChangeCallback.execute(status, reason, e, this.connectionStatusChangeCallbackContext);
        }
    }

    /**
     * Sleep for a length of time without interruption
     * @param sleepFor length of time to sleep for
     * @param unit time unit associated with sleepFor
     */
    private static void sleepUninterruptibly(long sleepFor, TimeUnit unit)
    {
        boolean interrupted = false;
        try
        {
            long remainingNanos = unit.toNanos(sleepFor);
            long end = System.nanoTime() + remainingNanos;
            while (true)
            {
                try
                {
                    NANOSECONDS.sleep(remainingNanos);
                    return;
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                    remainingNanos = end - System.nanoTime();
                }
            }
        }
        finally
        {
            if (interrupted)
            {
                Thread.currentThread().interrupt();
            }
        }
    }


}
