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
package com.informatica.um.binge.api.impl;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informatica.vds.api.VDSEvent;

/**
 * @author nraveend
 *
 */
public class VDSEventImpl implements VDSEvent {

    private ByteBuffer dataBuf;
    private Map<String, String> eventInfo; // Contains event specific headers
    private int dataLen;
    private static final Logger LOG = LoggerFactory.getLogger(VDSEventImpl.class);
    public VDSEventImpl(int blockSize) {
        dataBuf = ByteBuffer.allocate(blockSize);
        eventInfo = new HashMap<String, String>(10);
        dataLen = 0;
    }
    /* (non-Javadoc)
     * @see com.informatica.vds.api.VDSEvent#getStreamInfo()
     */
    @Override
    public Map<String, String> getEventInfo() {
        return Collections.unmodifiableMap(eventInfo);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.vds.api.VDSEvent#getBuffer()
     */
    @Override
    public ByteBuffer getBuffer() {
        dataBuf.position(0);
        return dataBuf;
    }

    /* (non-Javadoc)
     * @see com.informatica.vds.api.VDSEvent#getDataLen()
     */
    @Override
    public int getBufferLen() {
        return dataLen;
    }

    public void setDataLen(int dataLen) {
        this.dataLen = dataLen;
    }


    public void reset() {
        setDataLen(0);
        eventInfo.clear();
        dataBuf.rewind();
    }

    public int getBlockSize() {
        return dataBuf.capacity();
    }

    public void addHeaders(Map<String, String> headers) {
        eventInfo.putAll(headers);
    }

    public boolean hasHeaders() {
        return !eventInfo.isEmpty();
    }

    /**
     * Use this to set the data for the event
     * @param buf - data buffer
     * @param len - length of data in the buffer.
     */
    public void setData(byte[] src, int len) {
        dataBuf.put(src, 0, len);
        setDataLen(len);
    }

    /**
     * Set the data 
     * @param buf - buffer which contains data
     * @param len - Length of the buffer.
     */
    public void setData(ByteBuffer buf, int len) {
        buf.get(dataBuf.array(), 0, len);
        setDataLen(len);
    }

}
