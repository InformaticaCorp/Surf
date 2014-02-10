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
package com.informatica.binge.common;

import com.informatica.vds.api.VDSEvent;
import com.informatica.vds.api.VDSEventList;



public interface VDSMarshaller {

    /**
     * 
     * @param event
     *            - Data  to be marshaled
     * @return - length of the marshaled data.
     */
    int marshal(VDSEvent event);

    /**
     * @param src
     *          - data to be parsed in records
     * @param outEvents
     *          - list of parsed records, each being a VDSEvent
     * @throws Exception
     */
    void marshalByteBlockIntoIndividualRecords(VDSEvent src, VDSEventList outEvents) throws Exception;
}
