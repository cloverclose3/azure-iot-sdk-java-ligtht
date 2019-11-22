// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.sdk.iot.device;

import com.microsoft.azure.sdk.iot.device.exceptions.DeviceClientException;
import com.microsoft.azure.sdk.iot.device.transport.IotHubReceiveTask;
import com.microsoft.azure.sdk.iot.device.transport.IotHubSendTask;
import com.microsoft.azure.sdk.iot.device.transport.IotHubTransport;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/*
 *     +-------------------------------------+                  +-----------------------------------+
 *     |                                     |                  |                                   |
 *     |             DeviceClient            |------------------+        DeviceClientConfig         |
 *     |                                     |                  |                                   |
 *     +-------------------------------------+                  +-----------------------------------+
 *        |                        |
 *        |                       \/
 *        |  +---------------------------------------------------------------------------------------------+
 *        |  | Services                                                                                    |
 *        |  |  +-----------+    +------------+    +--------------+                        +------------+  |
 *        |  |  | Telemetry |    | DeviceTwin |    | DeviceMethod |                        | FileUpload |  |
 *        |  |  +-----------+    +------------+    +--------------+                        +---------+--+  |
 *        |  +---------------------------------------------------------------------------------------|-----+
 *        |                                    |                                                     |
 *       \/                                   \/                                                     |
 *     #####################################################################################         |
 *     # DeviceIO                                                                          #         |
 *     #  +----------------+    +-------------------------------------+    +------------+  #         |
 *     #  |                |    |                open                 |    |            |  #         |
 *     #  | sendEventAsync |    |                   +---------------+ |    |   close    |  #         |
 *     #  |                |    |                   | taskScheduler | |    |            |  #         |
 *     #  +--------+-------+    +--+----------------+--+---------+--+-+    +--------+---+  #         |
 *     ############|###############|###################|#########|##################|#######         |
 *                 |               |                   |         |                  |                |
 *                 |               |                  \/        \/                  |                |
 *                 |               |    +----------------+   +-------------------+  |                |
 *                 |               |    | IoTHubSendTask |   | IoTHubReceiveTask |  |                |
 *                 |               |    |   +--------+   |   |    +---------+    |  |                |
 *                 |               |    |   |   Run  |   |   |    |   Run   |    |  |                |
 *                 |               |    +---+---+----+---+   +----+----+----+----+  |                |
 * IotHubTransport |               |            |                      |            |                |
 *       +---------|---------------|------------|----------------------|------------|--------+   +----------------------------------------------+
 *       |        \/              \/           \/                     \/           \/        |   | IoTHubTransportManager                       |
 *       |  +------------+  +------------+  +--------------+  +---------------+  +---------+ |   |  +------+  +-------+  +------+  +---------+  |
 *       |  | addMessage |  |    Open    |  | sendMessages |  | handleMessage |  |  Close  | |   |  | Open |  | Close |  | send |  | receive |  |
 *       |  +------------+  +------------+  +--------------+  +---------------+  +---------+ |   |  +------+  +-------+  +------+  +---------+  |
 *       +----------+--------------------------------+-------------------------+-------------+   +---+------------------------------------------+
 *                  |                                |                         |                     |
 *                 \/                               \/                        \/                    \/
 *      +-------------------------+    +-------------------------+    +------------------+  +-----------------------+
 *      |      AmqpsTransport     |    |      MqttTransport      |    |  HttpsTransport  |  | HttpsTransportManager |
 *      +-------------------------+    +-------------------------+    +---------------------------------------------+
 *      |  AmqpsIotHubConnection  |    |  MqttIotHubConnection   |    |             HttpsIotHubConnection           |
 *      +-------------------------+    +-------------------------+    +---------------------------------------------+
 *
 */

/**
 * The task scheduler for sending and receiving messages for the Device Client
 */
public final class DeviceIO
{
    /** The state of the IoT Hub client's connection with the IoT Hub. */
    protected enum IotHubClientState
    {
        OPEN, CLOSED
    }

    private long sendPeriodInMilliseconds;
    private long receivePeriodInMilliseconds;

    private IotHubTransport transport;
    private DeviceClientConfig config;
    private IotHubSendTask sendTask = null;
    private IotHubReceiveTask receiveTask = null;

    private ScheduledExecutorService taskScheduler;
    private IotHubClientState state;

    /**
     * Constructor that takes a connection string as an argument.
     *
     * @param config the connection configuration.
     * @param sendPeriodInMilliseconds the period of time that iot hub will try to send messages in milliseconds.
     * @param receivePeriodInMilliseconds the period of time that iot hub will try to receive messages in milliseconds.
     *
     * @throws IllegalArgumentException if any of {@code config} or
     * {@code protocol} are {@code null}.
     */
    DeviceIO(DeviceClientConfig config, long sendPeriodInMilliseconds, long receivePeriodInMilliseconds)
    {
        /* Codes_SRS_DEVICE_IO_21_002: [If the `config` is null, the constructor shall throw an IllegalArgumentException.] */
        if(config == null)
        {
            throw new IllegalArgumentException("Config cannot be null.");
        }

        /* Codes_SRS_DEVICE_IO_21_001: [The constructor shall store the provided protocol and config information.] */
        this.config = config;

        /* Codes_SRS_DEVICE_IO_21_037: [The constructor shall initialize the `sendPeriodInMilliseconds` with default value of 10 milliseconds.] */
        this.sendPeriodInMilliseconds = sendPeriodInMilliseconds;
        /* Codes_SRS_DEVICE_IO_21_038: [The constructor shall initialize the `receivePeriodInMilliseconds` with default value of each protocol.] */
        this.receivePeriodInMilliseconds = receivePeriodInMilliseconds;

        /* Codes_SRS_DEVICE_IO_21_006: [The constructor shall set the `state` as `DISCONNECTED`.] */
        this.state = IotHubClientState.CLOSED;


        this.transport = new IotHubTransport(config);

        /* Codes_SRS_DEVICE_IO_21_037: [The constructor shall initialize the `sendPeriodInMilliseconds` with default value of 10 milliseconds.] */
        this.sendPeriodInMilliseconds = sendPeriodInMilliseconds;
        /* Codes_SRS_DEVICE_IO_21_038: [The constructor shall initialize the `receivePeriodInMilliseconds` with default value of each protocol.] */
        this.receivePeriodInMilliseconds = receivePeriodInMilliseconds;

        /* Codes_SRS_DEVICE_IO_21_006: [The constructor shall set the `state` as `DISCONNECTED`.] */
        this.state = IotHubClientState.CLOSED;
    }

    /**
     * Starts asynchronously sending and receiving messages from an IoT Hub. If
     * the client is already open, the function shall do nothing.
     *
     * @throws IOException if a connection to an IoT Hub cannot be established.
     */
    void open() throws IOException
    {
        /* Codes_SRS_DEVICE_IO_21_007: [If the client is already open, the open shall do nothing.] */
        if (this.state == IotHubClientState.OPEN)
        {
            return;
        }

        /* Codes_SRS_DEVICE_IO_21_012: [The open shall open the transport to communicate with an IoT Hub.] */
        /* Codes_SRS_DEVICE_IO_21_015: [If an error occurs in opening the transport, the open shall throw an IOException.] */
        try
        {
            this.transport.open();
        }
        catch (DeviceClientException e)
        {
            throw new IOException("Could not open the connection", e);
        }

        /* Codes_SRS_DEVICE_IO_21_014: [The open shall schedule receive tasks to run every receivePeriodInMilliseconds milliseconds.] */
        /* Codes_SRS_DEVICE_IO_21_016: [The open shall set the `state` as `CONNECTED`.] */
        commonOpenSetup();
    }

    /**
     * Handles logic common to all open functions.
     */
    private void commonOpenSetup()
    {
        this.sendTask = new IotHubSendTask(this.transport);
        this.receiveTask = new IotHubReceiveTask(this.transport);

        this.taskScheduler = Executors.newScheduledThreadPool(2);
        // the scheduler waits until each execution is finished before
        // scheduling the next one, so executions of a given task
        // will never overlap.
        /* Codes_SRS_DEVICE_IO_21_013: [The open shall schedule send tasks to run every SEND_PERIOD_MILLIS milliseconds.] */
        this.taskScheduler.scheduleAtFixedRate(this.sendTask, 0,
                sendPeriodInMilliseconds, TimeUnit.MILLISECONDS);
        /* Codes_SRS_DEVICE_IO_21_014: [The open shall schedule receive tasks to run every receivePeriodInMilliseconds milliseconds.] */
        this.taskScheduler.scheduleAtFixedRate(this.receiveTask, 0,
                receivePeriodInMilliseconds, TimeUnit.MILLISECONDS);

        /* Codes_SRS_DEVICE_IO_21_016: [The open shall set the `state` as `CONNECTED`.] */
        this.state = IotHubClientState.OPEN;
    }

    /**
     * Completes all current outstanding requests and closes the IoT Hub client.
     * Must be called to terminate the background thread that is sending data to
     * IoT Hub. After {@code close()} is called, the IoT Hub client is no longer
     *  usable. If the client is already closed, the function shall do nothing.
     *
     * @throws IOException if the connection to an IoT Hub cannot be closed.
     */
    public void close() throws IOException
    {
        /* Codes_SRS_DEVICE_IO_21_017: [The close shall finish all ongoing tasks.] */
        /* Codes_SRS_DEVICE_IO_21_018: [The close shall cancel all recurring tasks.] */
        if (taskScheduler != null)
        {
            this.taskScheduler.shutdown();
        }

        /* Codes_SRS_DEVICE_IO_21_019: [The close shall close the transport.] */
        try
        {
            this.transport.close(IotHubConnectionStatusChangeReason.CLIENT_CLOSE, null);
        }
        catch (DeviceClientException e)
        {
            this.state = IotHubClientState.CLOSED;
            throw new IOException(e);
        }

        /* Codes_SRS_DEVICE_IO_21_021: [The close shall set the `state` as `CLOSE`.] */
        this.state = IotHubClientState.CLOSED;
    }

    /**
     * Asynchronously sends an event message to the IoT Hub.
     *
     * @param message the message to be sent.
     * @param callback the callback to be invoked when a response is received.
     * Can be {@code null}.
     * @param callbackContext a context to be passed to the callback. Can be
     * {@code null} if no callback is provided.
     * @param deviceId the id of the device sending the message
     *
     * @throws IllegalArgumentException if the message provided is {@code null}.
     * @throws IllegalStateException if the client has not been opened yet or is already closed.
     */
    public synchronized void sendEventAsync(Message message,
                               IotHubEventCallback callback,
                               Object callbackContext,
                               String deviceId)
    {
        /* Codes_SRS_DEVICE_IO_21_024: [If the client is closed, the sendEventAsync shall throw an IllegalStateException.] */
        if (this.state == IotHubClientState.CLOSED)
        {
            throw new IllegalStateException(
                    "Cannot send event from "
                            + "an IoT Hub client that is closed.");
        }

        /* Codes_SRS_DEVICE_IO_21_023: [If the message given is null, the sendEventAsync shall throw an IllegalArgumentException.] */
        if (message == null)
        {
            throw new IllegalArgumentException("Cannot send message 'null'.");
        }


        /* Codes_SRS_DEVICE_IO_21_022: [The sendEventAsync shall add the message, with its associated callback and callback context, to the transport.] */
        transport.addMessage(message, callback, callbackContext);
    }


    /**
     * Getter for the connection state.
     *
     * @return a boolean true if the connection is open, or false if it is closed.
     */
    public boolean isOpen()
    {
        /* Codes_SRS_DEVICE_IO_21_031: [The isOpen shall return the connection state, true if connection is open, false if it is closed.] */
        return (this.state == IotHubClientState.OPEN);
    }

    public void registerConnectionStatusChangeCallback(IotHubConnectionStatusChangeCallback statusChangeCallback, Object callbackContext)
    {
        //Codes_SRS_DEVICE_IO_34_020: [This function shall register the callback with the transport.]
        this.transport.registerConnectionStatusChangeCallback(statusChangeCallback, callbackContext);
    }

}
