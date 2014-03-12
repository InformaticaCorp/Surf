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
package com.informatica.surf.transforms.partitionkey;


import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEvent;
import com.informatica.vds.api.VDSEventList;
import com.informatica.vds.api.VDSTransform;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a transform that takes in a String and extracts a partition key from it.
 * Kinesis uses partition keys to ensure that related records end up on the same shard. All records
 * having the same partition key are guaranteed to appear on the same shard. This transform uses a
 * customizable regex to extract a partition key from the message text. For each matched string, the
 * string matched by the first group (ie, between () parenthesis) is used as the partition key. Unmatched
 * lines are dropped.
 */

public class PartitionKeyTransform implements VDSTransform{
    private Pattern _pattern;
    private static final Logger _logger = LoggerFactory.getLogger(PartitionKeyTransform.class);

    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        String regex = ctx.optString("regex", ".* \"[A-Z]* ([^\" ]*) .*\".*");
        _pattern = Pattern.compile(regex);
        _logger.info("Extracting partition key using the regex: {}", regex);
    }

    @Override
    public void apply(VDSEvent inputEvent, VDSEventList outEvents) throws Exception {
        ByteBuffer buf = inputEvent.getBuffer();
        String data = new String(buf.array());
        StringTokenizer tok = new StringTokenizer(data, "\n");
        while(tok.hasMoreTokens()){
            String line = tok.nextToken();
            Matcher matcher = _pattern.matcher(line);
            if(matcher.matches()){
                String key = matcher.group(1);
                HashMap<String, String> map = new HashMap<>();
                map.put("kinesis-partition-key", key);
                byte []b = line.getBytes();
                outEvents.addEvent(b, b.length, map);
            }
            else{
                _logger.info("Dropping unmatched line: {}", line);
            }
        }

    }

    @Override
    public void close() throws IOException {

    }
}
