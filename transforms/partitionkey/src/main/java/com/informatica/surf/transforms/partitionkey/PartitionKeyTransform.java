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


public class PartitionKeyTransform implements VDSTransform{
    private Pattern _pattern;
    private static final Logger _logger = LoggerFactory.getLogger(PartitionKeyTransform.class);

    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        String regex = ctx.optString("partition-key-regex", ".* \"[A-Z]* ([^\" ]*) .*\".*");
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
