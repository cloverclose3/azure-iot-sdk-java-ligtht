/*
 *  Copyright (c) Microsoft. All rights reserved.
 *  Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package com.microsoft.azure.sdk.iot.device;

import com.microsoft.azure.sdk.iot.device.exceptions.TransportException;
import com.microsoft.azure.sdk.iot.device.transport.RetryPolicy;

import javax.net.ssl.SSLContext;
import java.io.IOError;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;


public class InternalClient
{
    DeviceClientConfig config;
    DeviceIO deviceIO;

    InternalClient(DeviceClientConfig config, long sendPeriodMillis, long receivePeriodMillis)
    {
        this.config = config;

        this.deviceIO = new DeviceIO(this.config, sendPeriodMillis, receivePeriodMillis);
    }

    public void open() throws IOException
    {
        this.deviceIO.open();
    }

    public void close() throws IOException
    {
        this.deviceIO.close();
    }

    public void closeNow() throws IOException
    {
        this.deviceIO.close();
    }

    public void sendEventAsync(Message message, IotHubEventCallback callback, Object callbackContext)
    {
        //Codes_SRS_INTERNALCLIENT_21_010: [The sendEventAsync shall asynchronously send the message using the deviceIO connection.]
        deviceIO.sendEventAsync(message, callback, callbackContext, this.config.getDeviceId());
    }

    /**
     * Registers a callback to be executed when the connection status of the device changes. The callback will be fired
     * with a status and a reason why the device's status changed. When the callback is fired, the provided context will
     * be provided alongside the status and reason.
     *
     * <p>Note that the thread used to deliver this callback should not be used to call open()/closeNow() on the client
     * that this callback belongs to. All open()/closeNow() operations should be done on a separate thread</p>
     *
     * @param callback The callback to be fired when the connection status of the device changes. Can be null to
     *                 unset this listener as long as the provided callbackContext is also null.
     * @param callbackContext a context to be passed to the callback. Can be {@code null}.
     * @throws IllegalArgumentException if provided callback is null
     */
    public void registerConnectionStatusChangeCallback(IotHubConnectionStatusChangeCallback callback, Object callbackContext) throws IllegalArgumentException
    {
        //Codes_SRS_INTERNALCLIENT_34_069: [This function shall register the provided callback and context with its device IO instance.]
        this.deviceIO.registerConnectionStatusChangeCallback(callback, callbackContext);
    }


    /**
     * Sets the message callback.
     *
     * @param callback the message callback. Can be {@code null}.
     * @param context the context to be passed to the callback. Can be {@code null}.
     *
     * @throws IllegalArgumentException if the callback is {@code null} but a context is
     * passed in.
     * @throws IllegalStateException if the callback is set after the client is
     * closed.
     */
    void setMessageCallbackInternal(MessageCallback callback, Object context)
    {
        if (callback == null && context != null)
        {
            /* Codes_SRS_INTERNALCLIENT_11_014: [If the callback is null but the context is non-null, the function shall throw an IllegalArgumentException.] */
            throw new IllegalArgumentException("Cannot give non-null context for a null callback.");
        }

        /* Codes_SRS_INTERNALCLIENT_11_013: [The function shall set the message callback, with its associated context.] */
        this.config.setMessageCallback(callback, context);
    }

    /**
     * Sets the given retry policy on the underlying transport
     * <a href="https://github.com/Azure/azure-iot-sdk-java/blob/master/device/iot-device-client/devdoc/requirement_docs/com/microsoft/azure/iothub/retryPolicy.md">
     *     See more details about the default retry policy and about using custom retry policies here</a>
     * @param retryPolicy the new interval in milliseconds
     */
    public void setRetryPolicy(RetryPolicy retryPolicy)
    {
        //Codes_SRS_INTERNALCLIENT_28_001: [The function shall set the device config's RetryPolicy .]
        this.config.setRetryPolicy(retryPolicy);
    }

}
