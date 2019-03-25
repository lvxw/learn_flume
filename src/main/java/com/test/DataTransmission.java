package com.test;

import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.client.avro.EventReader;
import org.apache.flume.client.avro.ReliableEventReader;
import org.apache.flume.client.avro.SimpleTextLineEventReader;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataTransmission {
    public static void main(String[] args) throws Exception{
        Map<String, String> headers = new HashMap<String, String>(1);
        headers.put("topic","test");

        RpcClient  rpcClient =  RpcClientFactory.getDefaultInstance("192.168.21.90", 1111);

        EventReader reader = new SimpleTextLineEventReader(new FileReader(new File("C:\\tmp\\logs\\a.txt")));
        int batchSize = 1000;
        List<Event> events;
        while (true) {
            while (!(events = reader.readEvents(batchSize)).isEmpty()) {
                for (Event event : events) {
                    event.setHeaders(headers);
                }
                rpcClient.appendBatch(events);
                if (reader instanceof ReliableEventReader) {
                    ((ReliableEventReader) reader).commit();
                }
            }
        }
    }
}
