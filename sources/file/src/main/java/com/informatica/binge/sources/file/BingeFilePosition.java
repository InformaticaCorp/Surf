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

import java.io.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informatica.um.binge.BingeUtils;

public class BingeFilePosition implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(BingeFilePosition.class);
    private File file;
    private RandomAccessFile rdwr;

    public BingeFilePosition(String path) throws Exception {
        File actual = new File(path);
        file = getPosFile(actual);
        rdwr = new RandomAccessFile(file, "rw");
    }

    @Override
    public void close() {
        BingeUtils.close(LOG, file.getAbsolutePath(), rdwr);
    }

    public static File getPosFile(File actual) {
        // no need for null check for parent path as we will be using absolute paths only
        return new File(actual.getParent(), String.format(".%s-bingepos", actual.getName()));
    }

    public long lastPosition() throws Exception {
        rdwr.seek(0);
        try {
            return rdwr.readLong();
        } catch (EOFException e) {
            // write 0 so that if we do this again, we won't have to deal with the exception
            LOG.warn("EOF encountered while reading position from file {}. resetting position to 0", file);
            update(0);
            return 0;
        } catch (Exception e) {
            throw e;
        }
    }

    public void update(long pos) throws IOException {
        rdwr.seek(0);
        rdwr.writeLong(pos);
    }

    public void unlink() {
        BingeUtils.close(LOG, file.getAbsolutePath(), rdwr);
        file.delete();
    }

    @Override
    public String toString() {
        return file.getAbsolutePath();
    }

}
