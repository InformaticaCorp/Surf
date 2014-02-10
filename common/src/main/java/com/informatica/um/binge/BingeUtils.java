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
package com.informatica.um.binge;

import java.io.Closeable;

import org.slf4j.Logger;

import com.informatica.um.binge.api.impl.PluginsFactory;
import com.informatica.vds.api.VDSException;

public class BingeUtils {
    private static int blkSize = 8192;

    private static final int MIN_CUSTOM_TYPE = 10000;
    private static final int MAX_CUSTOM_TYPE = 20000;

    // should be replaced with global configuration
    @Deprecated
    public static void setBlockSize(int b) {
        blkSize = b;
    }

    // should be replaced with global configuration
    @Deprecated
    public static int getBlockSize() {
        return blkSize;
    }

    // closes the stream/handle and logs at warn level in case of any error while closing.
    public static void close(Logger logger, String handleName, Closeable handle) {
        try {
            if (handle != null) {
                handle.close();
                logger.info("close completed for handle: {}", handleName);
            }
        } catch (Exception e) {
            logger.warn("error occurred while closing handle: {}", handleName, e);
        }
    }

    /**
     * re-throw in case of VDS exception. DON'T ABUSE this to suppress exceptions
     * @param e
     * @throws Exception
     * @see PluginsFactory#downloadPluginZip for correct usage
     */
    public static void rethrowVDSException(Exception e) throws Exception {
        if (e instanceof VDSException)
            throw e;
        return;
    }
}
