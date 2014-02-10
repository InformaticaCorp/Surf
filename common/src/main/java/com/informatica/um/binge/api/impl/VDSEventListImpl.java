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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;

import com.informatica.vds.api.VDSEventList;

/**
 * @author nraveend
 *
 */
public class VDSEventListImpl implements VDSEventList {

    private GenericKeyedObjectPool<Integer, VDSEventImpl> eventPool;
    static final int MIN_BUFFER_SIZE = 256;
    private List<VDSEventImpl> eventsList = Collections.synchronizedList(new ArrayList<VDSEventImpl>(10));

    public VDSEventListImpl(GenericKeyedObjectPool<Integer, VDSEventImpl> eventPool) {
        this.eventPool = eventPool;
    }
    /* (non-Javadoc)
     * @see com.informatica.vds.api.VDSEventList#addEvent(byte[], int)
     */
    @Override
    public void addEvent(byte[] buf, int len) throws Exception {
        addEventImpl(buf, len);
    }

    public VDSEventImpl addEventImpl(byte[] buf, int len) throws Exception {
        int eventSize = (len / MIN_BUFFER_SIZE) * MIN_BUFFER_SIZE + MIN_BUFFER_SIZE;
        VDSEventImpl event = eventPool.borrowObject(eventSize);
        event.setData(buf, len);
        eventsList.add(event);
        return event;
    }

    public List<VDSEventImpl> getEventsList() {
        return eventsList;
    }

    /**
     * Used by standard sources to avoid data copy while using vanila buffer array
     * @param event
     * @throws Exception 
     */
    public void addEvent(VDSEventImpl event) throws Exception {
        if (event.getBufferLen() > 0) {
            eventsList.add(event);
        } else {
            eventPool.returnObject(event.getBlockSize(), event);
        }
    }

    public VDSEventImpl getEvent(int eventSize) throws Exception {
        return eventPool.borrowObject(eventSize);
    }

    public void reset() {
        eventsList.clear();
    }

    @Override
    public void addEvent(byte[] buf, int len, Map<String, String> headers) throws Exception {
        VDSEventImpl eventImpl = addEventImpl(buf, len);
        eventImpl.addHeaders(headers);
    }

    public void copyEvent(VDSEventImpl event) throws Exception {
        VDSEventImpl copy = eventPool.borrowObject(event.getBlockSize());
        copy.setData(event.getBuffer(), event.getBufferLen());
        addEvent(copy);
    }
}
