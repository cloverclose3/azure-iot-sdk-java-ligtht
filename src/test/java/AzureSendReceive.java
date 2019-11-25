import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.exceptions.TransportException;
import com.microsoft.azure.sdk.iot.device.transport.*;
import com.microsoft.azure.sdk.iot.device.transport.mqtt.MqttIotHubConnection;
import com.microsoft.azure.sdk.iot.device.transport.mqtt.exceptions.MqttUnauthorizedException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Handles messages from an IoT Hub. Default protocol is to use
 * MQTT transport.
 */
public class AzureSendReceive
{

    public static MessageCallback newMessageCallback() {
        return new MessageCallback() {
            @Override
            public IotHubMessageResult execute(Message msg, Object context)
            {
                System.out.println(
                        "Received message with content: " + new String(msg.getBytes(), Message.DEFAULT_IOTHUB_MESSAGE_CHARSET));

                return IotHubMessageResult.COMPLETE;
            }
        };
    }


    public static IotHubConnectionStatusChangeCallback newConnectionStatusChangeCallback() {
        return new IotHubConnectionStatusChangeCallback() {
            @Override
            public void execute(IotHubConnectionStatus status, IotHubConnectionStatusChangeReason statusChangeReason, Throwable throwable, Object callbackContext)
            {
                System.out.println();
                System.out.println("CONNECTION STATUS UPDATE: " + status);
                System.out.println("CONNECTION STATUS REASON: " + statusChangeReason);
                System.out.println("CONNECTION STATUS THROWABLE: " + (throwable == null ? "null" : throwable.getMessage()));
                System.out.println();

                if (throwable != null)
                {
                    throwable.printStackTrace();
                }

                if (status == IotHubConnectionStatus.DISCONNECTED)
                {
                    //connection was lost, and is not being re-established. Look at provided exception for
                    // how to resolve this issue. Cannot send messages until this issue is resolved, and you manually
                    // re-open the device client
                }
                else if (status == IotHubConnectionStatus.DISCONNECTED_RETRYING)
                {
                    //connection was lost, but is being re-established. Can still send messages, but they won't
                    // be sent until the connection is re-established
                }
                else if (status == IotHubConnectionStatus.CONNECTED)
                {
                    //Connection was successfully re-established. Can send messages.
                }
            }
        };
    }


    public static void main(String[] args)
            throws IOException, URISyntaxException
    {
        DeviceClientConfig deviceConf = new DeviceClientConfig(
                "tcp://xxx.mqtt.iot.bj.baidubce.com:1883",
                "xxx/xxx",
                "xxx/xxx",
                "DeviceId-spouqxft0x",
                "DeviceID0001");


        DeviceClient client = new DeviceClient(deviceConf);
        System.out.println("Successfully created an IoT Hub client.");

        MessageCallback callback = newMessageCallback();
        client.setMessageCallback(callback, null);
        System.out.println("Successfully set message callback.");

        IotHubConnectionStatusChangeCallback statusCb = newConnectionStatusChangeCallback();
        client.registerConnectionStatusChangeCallback(statusCb, null);
        System.out.println("Successfully set status change callback.");

        client.setRetryPolicy(new ExponentialBackoffWithJitter());
        System.out.println("Successfully set retry policy.");

        client.open();

        System.out.println("Opened connection to IoT Hub.");

        System.out.println("Beginning to receive messages...");

        System.out.println("Sending the following event messages: ");


        String deviceId = "MyJavaDevice";
        double temperature = 0.0;
        double humidity = 0.0;

        for (int i = 0; i < 5; ++i)
        {
            temperature = 20 + Math.random() * 10;
            humidity = 30 + Math.random() * 20;

            String msgStr = "{\"deviceId\":\"" + deviceId +"\",\"messageId\":" + i + ",\"temperature\":"+ temperature +",\"humidity\":"+ humidity +"}";

            try
            {
                Message msg = new Message(msgStr);
                msg.setMessageId(java.util.UUID.randomUUID().toString());
                System.out.println(msgStr);
                client.sendEventAsync(msg);
            }

            catch (Exception e)
            {
                e.printStackTrace(); // Trace the exception
            }

        }

        System.out.println("Wait for 2 second(s) for response from the IoT Hub...");

        // Wait for IoT Hub to respond.
        try
        {
            Thread.sleep(2000);
        }

        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        System.out.println("In receive mode. Waiting for receiving C2D messages. Press ENTER to close");

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        // close the connection
        System.out.println("Closing");
        client.closeNow();

        System.out.println("Shutting down...");
    }
}
