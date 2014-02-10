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


/**
 * @author nraveend
 *
 */
public class VDSEventPoolFactory implements KeyedPoolableObjectFactory<Integer, VDSEventImpl> {

    @Override
    public void activateObject(Integer blockSize, VDSEventImpl event) throws Exception {
    }

    @Override
    public void destroyObject(Integer blockSize, VDSEventImpl event) throws Exception {
    }

    @Override
    public VDSEventImpl makeObject(Integer blockSize) throws Exception {
        return new VDSEventImpl(blockSize);
    }

    @Override
    public void passivateObject(Integer blockSize, VDSEventImpl event) throws Exception {
        event.reset();
    }

    @Override
    public boolean validateObject(Integer blockSize, VDSEventImpl arg1) {
        return true;
    }

}
