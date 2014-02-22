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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.lmax.disruptor.RingBuffer;

/**
 *
 * @author Informatica Corp
 */
public class RecordProcessorFactory implements IRecordProcessorFactory{
    private final RingBuffer<KinesisEvent> _buffer;

    public RecordProcessorFactory(RingBuffer<KinesisEvent> buffer){

        _buffer = buffer;
    }
    
    @Override
    public IRecordProcessor createProcessor() {
        return new RecordProcessor(_buffer);
    }
    
}
