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

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;


/**
 * @author nraveend
 *
 */
public class VDSEventListPoolFactory implements KeyedPoolableObjectFactory<Integer, VDSEventListImpl> {

    private GenericKeyedObjectPool<Integer, VDSEventImpl> eventPool;

    public VDSEventListPoolFactory(GenericKeyedObjectPool<Integer, VDSEventImpl> eventPool) {
        this.eventPool = eventPool;
    }

    @Override
    public void activateObject(Integer arg0, VDSEventListImpl events) throws Exception {

    }

    @Override
    public void destroyObject(Integer arg0, VDSEventListImpl events) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public VDSEventListImpl makeObject(Integer arg0) throws Exception {
        return new VDSEventListImpl(eventPool);
    }

    @Override
    public void passivateObject(Integer arg0, VDSEventListImpl events) throws Exception {
        for (VDSEventImpl event : events.getEventsList()) {
            eventPool.returnObject(event.getBlockSize(), event);
        }
        events.reset();

    }

    @Override
    public boolean validateObject(Integer arg0, VDSEventListImpl events) {
        return true;
    }

}
