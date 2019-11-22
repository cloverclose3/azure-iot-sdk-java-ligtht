// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.sdk.iot.device;


import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


public final class DeviceClient extends InternalClient implements Closeable
{
    public static long SEND_PERIOD_MILLIS = 10L;
    public static long RECV_PERIOD_MILLIS = 10L;


    public DeviceClient(DeviceClientConfig config) throws URISyntaxException, IllegalArgumentException
    {
        super(config, SEND_PERIOD_MILLIS, RECV_PERIOD_MILLIS);
    }

    public DeviceClient setMessageCallback(MessageCallback callback, Object context) throws IllegalArgumentException
    {
        this.setMessageCallbackInternal(callback, context);
        return this;
    }

    public void open() throws IOException
    {
        super.open();
        System.out.println("Device client opened successfully");
    }

    public void closeNow() throws IOException
    {
        System.out.println("Closing device client...");
        super.closeNow();


        System.out.println("Device client closed successfully");
    }

}