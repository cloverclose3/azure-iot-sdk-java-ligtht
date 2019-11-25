// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.sdk.iot.device;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

public class Message
{
    public static final Charset DEFAULT_IOTHUB_MESSAGE_CHARSET = StandardCharsets.UTF_8;
    private String messageId;
    private byte[] body;

    public Message()
    {
        initialize();
    }

    public Message(byte[] body)
    {
        if (body == null)
        {
            throw new IllegalArgumentException("Message body cannot be 'null'.");
        }

        initialize();
        this.body = body;
    }

    public Message(String body)
    {
        if (body == null)
        {
            throw new IllegalArgumentException("Message body cannot be 'null'.");
        }

        initialize();

        this.body = body.getBytes(DEFAULT_IOTHUB_MESSAGE_CHARSET);
    }

    public byte[] getBytes()
    {
        byte[] bodyClone = null;

        if (this.body != null) {
            bodyClone = Arrays.copyOf(this.body, this.body.length);
        }

        return bodyClone;
    }

    private void initialize()
    {
        this.messageId = UUID.randomUUID().toString();
    }

    public String getMessageId()
    {
        return messageId;
    }

    public void setMessageId(String messageId)
    {
        this.messageId = messageId;
    }




    @Override
    public String toString()
    {
        StringBuilder s = new StringBuilder();
        s.append(" Message details: ");
        if (this.messageId != null && !this.messageId.isEmpty())
        {
            s.append("Message Id [").append(this.messageId).append("] ");
        }

        return s.toString();
    }
}