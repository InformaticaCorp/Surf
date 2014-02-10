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
package com.informatica.surf.sources.dummy;

import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEventList;
import com.informatica.vds.api.VDSSource;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author jraj
 */
public class DummySource implements VDSSource{

    private final Map<String, String> _headers = new HashMap<>();
    public DummySource(){
        _headers.put("kinesis-partition-key", "1"); // Need a better way to specify this
    }
    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        
    }

    @Override
    public void read(VDSEventList readEvents) throws Exception {
        Thread.sleep(1000);
        Date d = new Date();
        byte []b = d.toString().getBytes();
        readEvents.addEvent(b, b.length, _headers);
    }

    @Override
    public void close() throws IOException {
        
    }

    
}
