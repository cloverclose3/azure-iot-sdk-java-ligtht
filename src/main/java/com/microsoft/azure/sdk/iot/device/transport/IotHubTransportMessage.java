// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.sdk.iot.device.transport;

import com.microsoft.azure.sdk.iot.device.*;

/**
 * Extends Message, adding transport artifacts.
 */
public class IotHubTransportMessage extends Message
{

    private String topic;
    private MessageCallback messageCallback;
    private Object messageCallbackContext;

    public IotHubTransportMessage(byte[] data, String topic)
    {
        super(data);
        this.topic = topic;
    }

    public MessageCallback getMessageCallback()
    {
        return messageCallback;
    }

    public void setMessageCallback(MessageCallback messageCallback)
    {
        this.messageCallback = messageCallback;
    }

    public Object getMessageCallbackContext()
    {
        return messageCallbackContext;
    }

    public void setMessageCallbackContext(Object messageCallbackContext)
    {
        this.messageCallbackContext = messageCallbackContext;
    }

    @Override
    public String toString()
    {
        return "topic: " + topic + " body: " + super.getBytes();
    }
}
