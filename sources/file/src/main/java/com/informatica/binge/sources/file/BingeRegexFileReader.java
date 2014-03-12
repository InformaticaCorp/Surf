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
import static com.informatica.um.binge.BingeConstants.SRC_CFG_REGEX_FILENAME;
import static com.informatica.um.binge.BingeConstants.SRC_FLIGHT_SIZE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

import java.io.File;
import java.io.FileFilter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Queue;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

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

public class BingeRegexFileReader extends VDSMessageAckSource {

    private static final Logger LOG = LoggerFactory.getLogger(BingeRegexFileReader.class);

    private String directory;
    private RandomAccessFile reader;
    private FileFilter filter;
    private BingeFilePosition posfile;
    private Queue<POS> posQ;
    private long curpos;

    private WatchService watchService;
    private WatchKey watchKey;

    private TreeSet<File> outstandingFiles = new TreeSet<File>(new FileComparator());
    private VDSMarshaller marshaler;

    public BingeRegexFileReader() {
    }

    private void openFile(String directory) throws Exception {
        File dir = new File(directory);
        File[] files = dir.listFiles(filter);
        File first = null;

        // no match found in the parent/source directory, then wait for the first file to be created.
        if (files.length == 0) {
            while (outstandingFiles.size() == 0) {
                processWatcher();
            }
            first = outstandingFiles.pollFirst();
        } else {
            // found some matches. sort them and add them to outstanding to process them later
            LOG.info("found {} files matching the file name criteria", files.length);
            Arrays.sort(files, new FileComparator());

            // check for pos file to defend restart scenarios.
            int idx = 0;
            int foundpos = -1;
            for (; idx < files.length; idx++) {
                File f = files[idx];
                // First check if it has a position file. If it does, we should resume from this file
                File posfile = BingeFilePosition.getPosFile(f);
                if (posfile.exists()) {
                    LOG.info("found position file {} for file {}", posfile.getName(), f.getName());
                    foundpos = idx;
                    break;
                }
            }

            if (foundpos == -1) {
                foundpos = 0;
                LOG.info("could not find position file for any of the files available. starting with oldest file: {}",
                        files[foundpos]);
            }

            first = files[foundpos];
            LOG.info("processing oldest file {}", first);

            for (int i = foundpos + 1; i < files.length; i++) {
                LOG.info("found file {}. will be processed later", files[i]);
                outstandingFiles.add(files[i]);
            }
        }

        posfile = new BingeFilePosition(first.getAbsolutePath());
        reader = new RandomAccessFile(first, "r");
        curpos = posfile.lastPosition();
        LOG.info("opening file {} from position {}", first, curpos);
        reader.seek(curpos);
    }

    public void read(VDSEventList vdsEvents) throws Exception {
        int flen = 0;
        long last_pos = curpos;
        VDSEventListImpl eventsImpl = (VDSEventListImpl) vdsEvents;
        VDSEventImpl vdsEvent = eventsImpl.getEvent(BingeUtils.getBlockSize());
        ByteBuffer dataBuf = vdsEvent.getBuffer();
        while (flen <= 0) {
            flen = reader.read(dataBuf.array(), 0, vdsEvent.getBlockSize());
            vdsEvent.setDataLen(flen);
            if (marshaler != null)
                flen = marshaler.marshal(vdsEvent);
            if (flen == 0 || flen == -1) {
                if (!outstandingFiles.isEmpty() && posfile.lastPosition() == curpos) {
                    LOG.info("Reached end of current file, moving on to next");
                    File f = outstandingFiles.pollFirst();
                    LOG.info("Next file is {}", f);
                    reader.close();
                    posfile.unlink();
                    posfile = new BingeFilePosition(f.getAbsolutePath());
                    reader = new RandomAccessFile(f, "r");
                    last_pos = 0;
                    curpos = 0;
                } else {
                    processWatcher();
                }
            }
            reader.seek(last_pos);
        }
        curpos += flen;
        vdsEvent.setDataLen(flen);
        eventsImpl.addEvent(vdsEvent);
        LOG.debug("current file position: {}", curpos);
        reader.seek(curpos);
    }

    private void processWatcher() throws Exception {
        WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
        if (key != null) {
            for (WatchEvent<?> event : key.pollEvents()) {
                Path p = (Path) event.context();
                File f = new File(directory, p.toString());
                LOG.debug("New file system event. type: {}, path: {}", event.kind(), p);
                if (filter.accept(f)) {
                    outstandingFiles.add(new File(directory, f.getName()));
                    LOG.info("detected new file creation: {}. adding it to outstanding files list to be processed.", f);
                }
            }
            key.reset();
        }
    }

    @Override
    public Object getInputObject() throws Exception {
        POS p = posQ.poll();
        if (p == null) {
            p = new POS(reader.getFilePointer());
        } else {
            p.setPosition(reader.getFilePointer());
        }
        return p;
    }

    @Override
    public void close() {
        BingeUtils.close(LOG, "position file", posfile);
        BingeUtils.close(LOG, "reader", reader);
        BingeUtils.close(LOG, "watcher service", watchService);
    }

    @Override
    public void updateInputObject(Object obj) throws Exception {
        POS pos = (POS) obj;
        posfile.update(pos.getPosition());
        // reuse the POS object. if we are full, we just let it dangle and get GCed
        posQ.offer(pos);
    }

    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        // parse all the configuration
        String directory = ctx.getString(SRC_CFG_DIRECTORY);
        int flightSize = ctx.optInt(SRC_FLIGHT_SIZE, 1000);
        String regex = ctx.getString(SRC_CFG_REGEX_FILENAME);

        // initialize
        this.directory = directory;
        // We need as many as the ULB flight size.
        this.posQ = new ArrayBlockingQueue<POS>(flightSize);
        this.marshaler = SeparatorMarshaller.getSeparatorMarshaller(ctx);

        try {
            this.filter = new RegexFileFilter(Pattern.compile(regex));
        } catch (PatternSyntaxException e) {
            throw new VDSException(VDSErrorCode.REGEX_SYNTAX_ERROR, regex);
        }

        File parentdir = new File(directory);

        // validations
        if (parentdir.exists() == false || parentdir.isDirectory() == false) {
            throw new VDSException(VDSErrorCode.DIRECTORY_NOT_FOUND, parentdir.getAbsolutePath());
        }

        // we need write permission to write pos file & execute permission to list files
        if (!(parentdir.canWrite() && parentdir.canExecute() && parentdir.canRead())) {
            throw new VDSException(VDSErrorCode.INSUFFICIENT_PERMISSIONS, directory, "rwx");
        }

        LOG.info("file(regex) source configuration. directory: {}, regex filename: {}", parentdir, regex);
        Path filepath = parentdir.toPath();
        watchService = filepath.getFileSystem().newWatchService();
        watchKey = filepath.register(watchService, ENTRY_CREATE);

        openFile(directory);
    }

    static class RegexFileFilter implements FileFilter {
        private Pattern pattern;

        public RegexFileFilter(Pattern pattern) {
            this.pattern = pattern;
        }

        @Override
        public boolean accept(File file) {
            // we need to filter normal files (but not directories/symlinks) which matches the expected pattern
            return file.isFile() && pattern.matcher(file.getName()).matches();
        }

    }

    static class FileComparator implements Comparator<File> {
        public int compare(File o1, File o2) {
            return Long.valueOf(o1.lastModified()).compareTo(Long.valueOf(o2.lastModified()));
        }
    }
}
