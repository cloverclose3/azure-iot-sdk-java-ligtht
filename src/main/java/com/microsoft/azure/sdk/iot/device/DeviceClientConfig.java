// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.sdk.iot.device;

import com.microsoft.azure.sdk.iot.device.transport.ExponentialBackoffWithJitter;
import com.microsoft.azure.sdk.iot.device.transport.RetryPolicy;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration settings for an IoT Hub client. Validates all user-defined
 * settings.
 */
public final class DeviceClientConfig
{
    private String hostUrl;
    private String username;
    private String password;
    private String clientID;
    private String deviceID;

    private RetryPolicy retryPolicy = new ExponentialBackoffWithJitter();

    /**
     * The callback to be invoked if a message is received.
     */
    private MessageCallback defaultDeviceTelemetryMessageCallback;
    /** The context to be passed in to the message callback. */
    private Object defaultDeviceTelemetryMessageContext;

    public DeviceClientConfig(String hostUrl,
                              String username,
                              String password,
                              String clientID,
                              String deviceID) throws IllegalArgumentException
    {
        this.hostUrl = hostUrl;
        this.username = username;
        this.password = password;
        this.clientID = clientID;
        this.deviceID = deviceID;
    }

    public String getHostUrl() {
        return hostUrl;
    }
    public String getUsername() {
        return username;
    }
    public String getPassword() {
        return password;
    }
    public String getClientID() {
        return clientID;
    }
    public String getDeviceId() {
        return deviceID;
    }

    public void setMessageCallback(MessageCallback callback, Object context)
    {
        this.defaultDeviceTelemetryMessageCallback = callback;
        this.defaultDeviceTelemetryMessageContext = context;
    }

    public MessageCallback getDeviceTelemetryMessageCallback()
    {
        return this.defaultDeviceTelemetryMessageCallback;

    }

    public Object getDeviceTelemetryMessageContext()
    {
        return this.defaultDeviceTelemetryMessageContext;

    }


    /**
     * Setter for RetryPolicy
     *
     * @param retryPolicy The types of retry policy to be used
     * @throws IllegalArgumentException if retry policy is null
     */
    public void setRetryPolicy(RetryPolicy retryPolicy) throws IllegalArgumentException
    {
        // Codes_SRS_DEVICECLIENTCONFIG_28_002: [This function shall throw IllegalArgumentException retryPolicy is null.]
        if (retryPolicy == null)
        {
            throw new IllegalArgumentException("Retry Policy cannot be null.");
        }

        // Codes_SRS_DEVICECLIENTCONFIG_28_003: [This function shall set retryPolicy.]
        this.retryPolicy = retryPolicy;
    }

    /**
     * Getter for RetryPolicy
     *
     * @return The value of RetryPolicy
     */
    public RetryPolicy getRetryPolicy()
    {
        // Codes_SRS_DEVICECLIENTCONFIG_28_004: [This function shall return the saved RetryPolicy object.]
        return this.retryPolicy;
    }
}
