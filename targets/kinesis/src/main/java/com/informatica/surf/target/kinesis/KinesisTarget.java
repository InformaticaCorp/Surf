/* 
 * Copyright 2014 Informatica Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.informatica.surf.target.kinesis;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEvent;
import com.informatica.vds.api.VDSTarget;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jraj
 */
public class KinesisTarget implements VDSTarget{

    public static final String ACCESS_KEY = "aws-access-key-id";
    public static final String SECRET_KEY = "aws-secret-key";
    public static final String STREAM_NAME = "aws-kinesis-stream-name";
    public static final String PARTITION_KEY = "kinesis-partition-key";
    public static final String SEQUENCE_NUMBER = "kinesis-sequence-number";
    public static final String THREAD_COUNT = "kinesis-parallel-requests";
    
    private static final Logger _logger = LoggerFactory.getLogger(KinesisTarget.class);
    private AmazonKinesisAsyncClient _client;
    private String _streamName;
    private final Callback _callback = new Callback();
    private ExecutorService _threadpool;
    private final ScheduledExecutorService _scheduler = Executors.newSingleThreadScheduledExecutor();
    private String _partitionKey;
    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        String accessID = ctx.getString(ACCESS_KEY);
        String secretKey = ctx.getString(SECRET_KEY);
        _streamName = ctx.getString(STREAM_NAME);
        int tcount = ctx.optInt(THREAD_COUNT, 5);
        _threadpool = new ThreadPoolExecutor(tcount, tcount, 10, 
            TimeUnit.SECONDS, new ArrayBlockingQueue(100),new ThreadPoolExecutor.CallerRunsPolicy()); // TODO: make the queue length configurable        
        BasicAWSCredentials creds = new BasicAWSCredentials(accessID, secretKey);
        _client = new AmazonKinesisAsyncClient(creds, _threadpool);
        _scheduler.scheduleAtFixedRate(_callback, 10, 10, TimeUnit.SECONDS); // TODO: make this configurable?
        
        _partitionKey = ctx.getString(PARTITION_KEY);
        _logger.info("Created connection to AWS Kinesis");
        _logger.info("Stream name: " + _streamName);
    }

    @Override
    public void write(VDSEvent event) throws Exception {
        Map <String, String> headers = event.getEventInfo();
        PutRecordRequest req = new PutRecordRequest();
        req.setStreamName(_streamName);
        req.setData(event.getBuffer());
        if(headers != null){
            if(headers.containsKey(PARTITION_KEY)){
                req.setPartitionKey(headers.get(PARTITION_KEY));
            }
            else{
                req.setPartitionKey(_partitionKey);
            }
            if(headers.containsKey(SEQUENCE_NUMBER)){
                req.setSequenceNumberForOrdering(headers.get(SEQUENCE_NUMBER));
            }
        }
        _client.putRecordAsync(req, _callback);
    }

    @Override
    public void close() throws IOException {
        _logger.info("Closing AWS Kinesis connection");
    }
    
    class Callback implements AsyncHandler<PutRecordRequest, PutRecordResult>, Runnable{

        private final AtomicInteger count = new AtomicInteger(0);
        private final AtomicLong timestamp = new AtomicLong(System.currentTimeMillis());
        @Override
        public void onError(Exception excptn) {
            _logger.warn("An exception occurred when sending data to Kinesis", excptn);
        }

        @Override
        public void onSuccess(PutRecordRequest rqst, PutRecordResult result) {
            _logger.debug("PutRecord completed successfully. Sequence number = {}", result.getSequenceNumber());
            count.incrementAndGet();
        }
        
        @Override
        public void run(){
            long cnt = count.getAndSet(0);
            long cur = System.currentTimeMillis();
            long ts = timestamp.getAndSet(cur);
            long elapsed = cur - ts;
            long esec = elapsed/1000;
            long mps = cnt/esec;
            _logger.info("{} messages sent in the last {} seconds. {} messages/sec", cnt, elapsed/1000, mps);
        }
        
    }
    
}
