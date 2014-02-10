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

package com.informatica.surf.sources.http;

import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEventList;
import com.informatica.vds.api.VDSSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 *
 * @author Informatica Corp
 */
public class HttpSource implements VDSSource{
    /**
     * The HttpSource handles both normal HTTP POST requests, as well as websocket messages
     * It needs the following configuration:
     *    http-port: an integer port to listen on
     *    post-paths: a comma-separated list of paths to accept POST requests
     *    websocket-paths: a comma-separated list of paths to accept websocket requests
     * 
     * TODO:
     *   - Include HTTPS support
     *   - Tune threadpools etc for performance, and make them configurable
     *   - Wait for consumption report before sending HTTP response
     */
    private HttpListener _listener;
    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        int port = ctx.getInt("http-port");
        String ppaths = ctx.getString("post-paths");
        String wpaths = ctx.getString("websocket-paths");
        List<String> plist, wlist;
        plist = getPathsAsList(ppaths);
        wlist = getPathsAsList(wpaths);
        _listener = new HttpListener(port, plist, wlist);
        _listener.start();
        
    }
    
    /**
     * Utility method to break comma-separated paths into individual Strings
     * @param paths Comma-separated paths
     * @return List with individual paths
     */
    private List<String>getPathsAsList(String paths){
        List<String> plist = new ArrayList<>();
        StringTokenizer tok = new StringTokenizer(paths, ",");
        while(tok.hasMoreTokens()){
            plist.add(tok.nextToken().trim());
        }
        return plist;
    }

    @Override
    public void read(VDSEventList readEvents) throws Exception {
        //TODO: fix this to return all available events instead of just 1 at a time
        byte buf[];
        buf = _listener.nextMessage();
        readEvents.addEvent(buf, buf.length);
    }

    @Override
    public void close() throws IOException {
        _listener.stop();
    }
    
}
