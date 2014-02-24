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

package com.informatica.surf.sample;

import com.google.common.collect.HashMultiset;
import com.lmax.disruptor.EventHandler;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.StringTokenizer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Informatica Corp
 */
class PageCountHandler implements EventHandler<KinesisEvent>, Runnable {
    private final HashMultiset<String> _counts = HashMultiset.create();
    private final Pattern _pattern = Pattern.compile(".* \"[A-Z]* ([^\" ]*) .*\".*");
    private static final Logger _logger = LoggerFactory.getLogger(PageCountHandler.class);
    private final ScheduledExecutorService _executor = Executors.newScheduledThreadPool(1);
    public PageCountHandler() {
        _executor.scheduleAtFixedRate(this, 10, 10, TimeUnit.SECONDS);
    }

    @Override
    public void onEvent(KinesisEvent t, long l, boolean bln) throws Exception {
        String data = t.getData();
        _logger.debug("Got an event: {}", data);
        // data may consist of multiple lines
        StringTokenizer tok = new StringTokenizer(data, "\n");
        while(tok.hasMoreTokens()){
            String line = tok.nextToken();
            _logger.debug("Line = {}", line);
            Matcher m = _pattern.matcher(line);
            if(m.matches()){
                String page = m.group(1);
                _logger.debug("Found page {}", page);
                if(page != null){
                    _counts.add(page);
                }
            }
            else{
                _logger.debug("Unmatched log line");
            }
        }
    }

    @Override
    public void run() {
        try(PrintWriter output = new PrintWriter(new FileWriter("pagecount.html"))) {
            output.println("<html>\n<body>\n<table>");
            for(String page: _counts.elementSet()){
                output.printf("<tr><td>%s</td><td>%d</td></tr>\n", page, _counts.count(page));                
            }
            output.println("</table>\n</body>\n</html>");
        }
        catch(IOException ex){
            _logger.warn("Exception while writing output file", ex);
        }
    }
    
}
