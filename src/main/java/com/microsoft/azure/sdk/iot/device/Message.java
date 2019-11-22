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
    // ----- Constants -----

    public static final Charset DEFAULT_IOTHUB_MESSAGE_CHARSET = StandardCharsets.UTF_8;

    // ----- Data Fields -----

    private String messageId;



    /**
     * The message body
     */
    private byte[] body;


    // ----- Constructors -----

    /**
     * Constructor.
     */
    public Message()
    {
        initialize();
    }

    /**
     * Constructor.
     * @param body The body of the new Message instance.
     */
    public Message(byte[] body)
    {
        // Codes_SRS_MESSAGE_11_025: [If the message body is null, the constructor shall throw an IllegalArgumentException.]
        if (body == null)
        {
            throw new IllegalArgumentException("Message body cannot be 'null'.");
        }

        initialize();

        // Codes_SRS_MESSAGE_11_024: [The constructor shall save the message body.]
        this.body = body;
    }

    /**
     * Constructor.
     * @param body The body of the new Message instance. It is internally serialized to a byte array using UTF-8 encoding.
     */
    public Message(String body)
    {
        if (body == null)
        {
            throw new IllegalArgumentException("Message body cannot be 'null'.");
        }

        initialize();

        this.body = body.getBytes(DEFAULT_IOTHUB_MESSAGE_CHARSET);
    }


    // ----- Public Methods -----
    /**
     * The byte content of the body.
     * @return A copy of this Message body, as a byte array.
     */
    public byte[] getBytes()
    {
        // Codes_SRS_MESSAGE_11_002: [The function shall return the message body.]
        byte[] bodyClone = null;

        if (this.body != null) {
            bodyClone = Arrays.copyOf(this.body, this.body.length);
        }

        return bodyClone;
    }

    // ----- Private Methods -----
    /**
     * Internal initializer method for a new Message instance.
     */
    private void initialize()
    {
        this.messageId = UUID.randomUUID().toString();
    }

    /**
     * Getter for the messageId property
     * @return The property value
     */
    public String getMessageId()
    {
        // Codes_SRS_MESSAGE_34_043: [The function shall return the message's message Id.]
        return messageId;
    }

    /**
     * Setter for the messageId property
     * @param messageId The string containing the property value
     */
    public void setMessageId(String messageId)
    {
        // Codes_SRS_MESSAGE_34_044: [The function shall set the message's message ID to the provided value.]
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