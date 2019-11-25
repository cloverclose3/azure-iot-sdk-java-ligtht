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
        if(config == null)
        {
            throw new IllegalArgumentException("Config cannot be null.");
        }

        this.config = config;
        this.sendPeriodInMilliseconds = sendPeriodInMilliseconds;
        this.receivePeriodInMilliseconds = receivePeriodInMilliseconds;
        this.state = IotHubClientState.CLOSED;
        this.transport = new IotHubTransport(config);
    }

    /**
     * Starts asynchronously sending and receiving messages from an IoT Hub. If
     * the client is already open, the function shall do nothing.
     *
     * @throws IOException if a connection to an IoT Hub cannot be established.
     */
    void open() throws IOException
    {
        if (this.state == IotHubClientState.OPEN)
        {
            return;
        }

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

        this.state = IotHubClientState.OPEN;
    }

    public void close() throws IOException
    {
        if (taskScheduler != null)
        {
            this.taskScheduler.shutdown();
        }

        try
        {
            this.transport.close(IotHubConnectionStatusChangeReason.CLIENT_CLOSE, null);
        }
        catch (DeviceClientException e)
        {
            this.state = IotHubClientState.CLOSED;
            throw new IOException(e);
        }

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
                               String deviceId)
    {
        if (this.state == IotHubClientState.CLOSED)
        {
            throw new IllegalStateException(
                    "Cannot send event from "
                            + "an IoT Hub client that is closed.");
        }

        if (message == null)
        {
            throw new IllegalArgumentException("Cannot send message 'null'.");
        }


        transport.addMessage(message);
    }


    public boolean isOpen()
    {
        return (this.state == IotHubClientState.OPEN);
    }

    public void registerConnectionStatusChangeCallback(IotHubConnectionStatusChangeCallback statusChangeCallback, Object callbackContext)
    {
        this.transport.registerConnectionStatusChangeCallback(statusChangeCallback, callbackContext);
    }

}
