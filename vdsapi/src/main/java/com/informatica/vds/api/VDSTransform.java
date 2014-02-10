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
import java.io.IOException;



/**
 * Interface specification for custom VDS transformations. 
 * 
 */
public interface VDSTransform extends Closeable {

    /**
     * Opens a new VDSTransform object. It can be configured using the ctx object.
     * All the initialization required for the target should be done here.
     * @param ctx - Configuration context object
     */
    public void open(VDSConfiguration ctx) throws Exception;

    /**
     *  Apply the transformation to generate output.
     * @param inputEvent- Contains the data which needs to be transformed.
     * @param outEvents - Transformed VDSEventList
     * @throws IOException
     */
    void apply(VDSEvent inputEvent, VDSEventList outEvents) throws Exception;

}
