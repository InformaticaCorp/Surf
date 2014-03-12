package com.informatica.surf.target.dump;

import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEvent;
import com.informatica.vds.api.VDSTarget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by jerry on 12/03/14.
 */
public class Dump implements VDSTarget {
    private static final Logger _logger = LoggerFactory.getLogger(Dump.class);
    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        _logger.info("Dump target opened. Will log every event received");
    }

    @Override
    public void write(VDSEvent strm) throws Exception {
        String event = new String(strm.getBuffer().array(), 0, strm.getBufferLen());
        _logger.info("Event Data: {}", event);
    }

    @Override
    public void close() throws IOException {
        _logger.info("Dump target closed");
    }
}
