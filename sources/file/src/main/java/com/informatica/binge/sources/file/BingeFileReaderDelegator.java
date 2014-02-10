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
package com.informatica.binge.sources.file;

import static com.informatica.um.binge.BingeConstants.SRC_CFG_FILENAME;

import java.io.IOException;

import com.informatica.um.binge.api.impl.VDSMessageAckSource;
import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEventList;

public class BingeFileReaderDelegator extends VDSMessageAckSource {

    protected VDSMessageAckSource delegatee;

    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        // "filename" - key is used as a indicator to differentiate between raw file source vs regex file source
        if (ctx.contains(SRC_CFG_FILENAME)) {
            delegatee = new BingeFileReader();
        } else {
            delegatee = new BingeRegexFileReader();
        }
        delegatee.open(ctx);
    }

    @Override
    public void read(VDSEventList readEvents) throws Exception {
        delegatee.read(readEvents);
    }

    @Override
    public void close() throws IOException {
        delegatee.close();
    }

    @Override
    public void updateInputObject(Object obj) throws Exception {
        delegatee.updateInputObject(obj);
    }

    @Override
    public Object getInputObject() throws Exception {
        return delegatee.getInputObject();
    }
}
