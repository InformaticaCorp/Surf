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

import static com.informatica.um.binge.BingeConstants.SRC_CFG_DIRECTORY;
import static com.informatica.um.binge.BingeConstants.SRC_CFG_FILENAME;
import static com.informatica.um.binge.BingeConstants.SRC_FLIGHT_SIZE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.*;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informatica.binge.common.SeparatorMarshaller;
import com.informatica.binge.common.VDSMarshaller;
import com.informatica.um.binge.BingeUtils;
import com.informatica.um.binge.api.impl.VDSEventImpl;
import com.informatica.um.binge.api.impl.VDSEventListImpl;
import com.informatica.um.binge.api.impl.VDSMessageAckSource;
import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSErrorCode;
import com.informatica.vds.api.VDSEventList;
import com.informatica.vds.api.VDSException;

public class BingeFileReader extends VDSMessageAckSource {
    private static final Logger LOG = LoggerFactory.getLogger(BingeFileReader.class);

    private File file;
    private AsynchronousFileChannel reader;

    private WatchService watchService;
    private WatchKey watchKey;

    private BingeFilePosition posfile;
    private Queue<POS> posQ;
    private long pos;

    private VDSMarshaller marshaller;

    private boolean created = false;

    public BingeFileReader() {
    }

    @Override
    public void updateInputObject(Object obj) throws IOException {
        POS pos = (POS) obj;
        LOG.debug("source send completed. position: {}", pos.getPosition());
        posfile.update(pos.getPosition());
        // reuse the POS object
        // if we are full, we just let it dangle and get GCed
        posQ.offer(pos);
    }

    public void close() {
        if (watchKey != null && watchKey.isValid()) {
            watchKey.cancel();
        }
        BingeUtils.close(LOG, "position file", posfile);
        BingeUtils.close(LOG, "reader", reader);
        BingeUtils.close(LOG, "watch service", watchService);
    }

    public Object getInputObject() {
        POS p = posQ.poll();
        if (p == null)
            p = new POS(pos);
        else
            p.setPosition(pos);
        return p;
    }

    public void read(VDSEventList vdsEvents) throws Exception {
        int flen = 0;
        VDSEventListImpl eventsImpl = (VDSEventListImpl) vdsEvents;
        VDSEventImpl vdsEvent = eventsImpl.getEvent(BingeUtils.getBlockSize());
        ByteBuffer buf = vdsEvent.getBuffer();
        buf.mark();

        while (flen <= 0) {
            buf.reset();
            Future<Integer> future = reader.read(buf, pos);
            flen = 0;
            try {
                Integer ret = future.get(10, TimeUnit.SECONDS);
                flen = ret.intValue();
            } catch (TimeoutException ex) {
                // No data in the last 10 seconds. continue to check if reopen is required
                // cancel this old task: we will create a new one next time
                LOG.debug("No new data found in file {}. Polling again for new data.", file);
                future.cancel(false);
            }

            vdsEvent.setDataLen(flen);
            if (marshaller != null)
                flen = marshaller.marshal(vdsEvent);
            if (flen == 0 || flen == -1) {
                // if we got no data, it could be because the file was renamed and recreated (ie, rollover). If so, we
                // would have received a CREATED event earlier
                if (created && posfile.lastPosition() == pos) {
                    // close current file as targets have consumed the data
                    reader.close();
                    LOG.info("file processed completely, read {} bytes so far. reopening for new data {}",
                            posfile.lastPosition(), file);
                    reader = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ);
                    posfile.update(0);
                    pos = 0;
                    created = false;
                } else {
                    // See if any events have been queued
                    WatchKey k = watchService.poll(1, TimeUnit.SECONDS);
                    if (k != null) {
                        for (WatchEvent<?> event : k.pollEvents()) {
                            Path p = (Path) event.context();
                            LOG.debug("New file system event. type: {}, path: {}", event.kind(), p);
                            // we do register only for create events. so no need to handle other events
                            if (file.getName().equals(p.toString())) {
                                created = true;
                                break;
                            }
                        }
                        k.reset();
                    }
                }
            }
        }
        pos += flen;
        vdsEvent.setDataLen(flen);
        eventsImpl.addEvent(vdsEvent);
    }

    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        int flightSize = ctx.getInt(SRC_FLIGHT_SIZE);
        String path = new File(ctx.getString(SRC_CFG_DIRECTORY), ctx.getString(SRC_CFG_FILENAME)).getAbsolutePath();

        marshaller = SeparatorMarshaller.getSeparatorMarshaller(ctx);
        posQ = new ArrayBlockingQueue<POS>(flightSize); // We need as many as the ULB flight size.
        file = new File(path).getAbsoluteFile();

        File parentdir = file.getParentFile();
        if (parentdir.exists() == false || parentdir.isDirectory() == false) {
            throw new VDSException(VDSErrorCode.DIRECTORY_NOT_FOUND, parentdir.getAbsolutePath());
        }

        // we need write permission to write pos file & execute permission to list files
        if (!(parentdir.canRead() && parentdir.canWrite() && parentdir.canExecute())) {
            throw new VDSException(VDSErrorCode.INSUFFICIENT_PERMISSIONS, parentdir.getAbsolutePath(), "rwx");
        }

        if (file.exists() && file.canRead() == false) {
            throw new VDSException(VDSErrorCode.INSUFFICIENT_PERMISSIONS, file.getAbsolutePath(), "r");
        }

        LOG.info("file source configuration. directory: {}, filename: {}", parentdir, file.getName());

        Path filepath = parentdir.toPath();
        watchService = filepath.getFileSystem().newWatchService();
        watchKey = filepath.register(watchService, ENTRY_CREATE);

        // wait for file if not exists
        if (!file.exists()) {
            LOG.info("input file {} does not exist. waiting for file creation...", file);
            // rechecking file exists to defend in case of event miss
            while (!file.exists()) {
                WatchKey k = watchService.poll(1, TimeUnit.SECONDS);
                if (k != null) {
                    for (WatchEvent<?> event : k.pollEvents()) {
                        Path p = (Path) event.context();
                        LOG.debug("New file system event. type: {}, path: {}", event.kind(), p);
                        // we do register only for create events. so no need to handle other events
                        if (file.getName().equals(p.toString())) {
                            break;
                        }
                    }
                    k.reset();
                }
            }
        }

        LOG.info("input file {} available for read. opening it", file);
        reader = AsynchronousFileChannel.open(file.toPath(), StandardOpenOption.READ);

        posfile = new BingeFilePosition(file.getAbsolutePath());
        pos = posfile.lastPosition();
        LOG.info("starting to read file {} from position {}", file, pos);
    }

}
