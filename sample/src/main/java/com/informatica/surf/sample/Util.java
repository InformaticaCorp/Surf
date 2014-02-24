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

package com.informatica.surf.sample;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 *
 * @author Informatica Corp
 */
public class Util {
    
    public static Worker createWorker(File conf, EventHandler<KinesisEvent> handler, String appName)throws IOException{
        Executor executor = Executors.newCachedThreadPool();
        Disruptor<KinesisEvent> disruptor = new Disruptor<>(KinesisEvent.EVENT_FACTORY, 128, executor);

        disruptor.handleEventsWith(handler);
        RingBuffer<KinesisEvent> buffer = disruptor.start();

        Properties props = new Properties();
        props.load(new FileReader(conf));
        // Generate a unique worker ID
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        String accessid = props.getProperty("aws-access-key-id");
        String secretkey = props.getProperty("aws-secret-key");
        String streamname = props.getProperty("aws-kinesis-stream-name");
        BasicAWSCredentials creds = new BasicAWSCredentials(accessid, secretkey);
        CredProvider credprovider = new CredProvider(creds);
        KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(appName, streamname,  credprovider, workerId);
        
        Worker worker = new Worker(new RecordProcessorFactory(buffer), config, new MetricsFactory());
        return worker;
    }
    static class CredProvider implements AWSCredentialsProvider{
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
