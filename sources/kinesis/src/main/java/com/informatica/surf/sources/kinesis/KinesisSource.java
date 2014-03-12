package com.informatica.surf.sources.kinesis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEventList;
import com.informatica.vds.api.VDSSource;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by jerry on 11/03/14.
 */
public class KinesisSource implements VDSSource {
    private Worker _worker;
    private final ArrayBlockingQueue<KinesisEvent> _eventList = new ArrayBlockingQueue<>(1000);
    private final int MAX_EVENTS = 100; // Maximum number of events to send to Surf at one time; also max records fetched from Kinesis at a time
    private static final Logger _logger = LoggerFactory.getLogger(KinesisSource.class);
    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        _logger.info("Initializing Kinesis Source");
        Executor executor = Executors.newCachedThreadPool();
        Disruptor<KinesisEvent> disruptor = new Disruptor<>(KinesisEvent.EVENT_FACTORY, 128, executor);
        disruptor.handleEventsWith(new EventHandler<KinesisEvent>() {
            @Override
            public void onEvent(KinesisEvent kinesisEvent, long l, boolean b) throws Exception {
                _logger.debug("Received a Kinesis Event");
                _eventList.put(kinesisEvent);
                _logger.debug("eventlist size is now {}", _eventList.size());
            }
        });
        RingBuffer<KinesisEvent> buffer = disruptor.start();

        // Generate a unique worker ID
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        String accessid = ctx.getString("aws-access-key-id");
        String secretkey = ctx.getString("aws-secret-key");
        String streamname = ctx.getString("aws-kinesis-stream-name");
        String appName = ctx.getString("application-name");
        BasicAWSCredentials creds = new BasicAWSCredentials(accessid, secretkey);
        CredProvider credprovider = new CredProvider(creds);
        KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(appName, streamname,  credprovider, workerId)
                .withMaxRecords(MAX_EVENTS);

        _worker = new Worker(new RecordProcessorFactory(buffer), config, new MetricsFactory());
        new Thread(_worker).start();
    }

    @Override
    public void read(VDSEventList readEvents) throws Exception {
        int i = 0;
        while(i < MAX_EVENTS){
            KinesisEvent evt = _eventList.poll(300, TimeUnit.MILLISECONDS);
            if(evt == null){
                _logger.warn("Got a null event");
                break;
            }
            byte buf[] = evt.getData().getBytes();
            readEvents.addEvent(buf, buf.length);
            i++;
        }
    }

    @Override
    public void close() throws IOException {
        _worker.shutdown();

    }
    static class CredProvider implements AWSCredentialsProvider {
        AWSCredentials _creds;
        public CredProvider(AWSCredentials creds){
            _creds = creds;
        }
        @Override
        public AWSCredentials getCredentials() {
            return _creds;
        }

        @Override
        public void refresh() {
            // NOOP
        }

    }
}
