package com.test;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class MyApp {
    public static void main(String[] args) throws Exception{
        MyRpcClientFacade client = new MyRpcClientFacade();
        client.init("master", 1111);

        String sampleData = "Hello Flume!";
        for (int i = 0; i<5; i++) {
            Thread.sleep(1000);
            client.sendDataToFlume(sampleData+System.currentTimeMillis());
        }

    }
}

class MyRpcClientFacade {
    private RpcClient client;
    private String hostname;
    private int port;

    public void init(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
        this.client = RpcClientFactory.getDefaultInstance(hostname, port);
    }

    public void sendDataToFlume(String data) {
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("topic", "test");
        event.setHeaders(headers);
        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            e.printStackTrace();
        }
    }

    public void cleanUp() {
        client.close();
    }

}