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
package com.informatica.vds.api;

import java.io.Closeable;


public interface VDSSource extends Closeable {

    /**
     * Opens a new VDSSource object. It can be configured using the ctx object.
     * All the initialization required for the target should be done here.
     * @param ctx - Configuration context object
     */
    public void open(VDSConfiguration ctx) throws Exception;

    /**
     * 
     * @param readEvents - object to which the readEvents can be added
     * @throws Exception
     */
    public void read(VDSEventList readEvents) throws Exception;
}
