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

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Informatica Corp
 */
class RecordProcessor implements IRecordProcessor {
    private static final Logger _logger = LoggerFactory.getLogger(RecordProcessor.class);

    public RecordProcessor() {
    }

    @Override
    public void initialize(String string) {
        _logger.info("RecordProcessor initialized");
    }

    @Override
    public void processRecords(List<Record> list, IRecordProcessorCheckpointer irpc) {        
        _logger.info("Processing {} records", list.size());
        for(Record r: list){
            String data = new String(r.getData().array());
            System.out.println(String.format("seqno %s: partkey %s: %s", r.getSequenceNumber(), r.getPartitionKey(), data));
        }
        try{
            irpc.checkpoint();
        }
        catch(InvalidStateException | KinesisClientLibDependencyException | ShutdownException | ThrottlingException ex){
            _logger.warn("Exception while checkpointing", ex);
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer irpc, ShutdownReason sr) {
        _logger.info("Shutting down record processor");
    }
    
}
