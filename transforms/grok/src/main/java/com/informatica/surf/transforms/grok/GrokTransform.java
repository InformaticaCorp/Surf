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

package com.informatica.surf.transforms.grok;

import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEvent;
import com.informatica.vds.api.VDSEventList;
import com.informatica.vds.api.VDSTransform;
import com.nflabs.grok.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Created by jerry on 02/04/14.
 */
public class GrokTransform implements VDSTransform {
    private com.nflabs.grok.Grok _grok = new com.nflabs.grok.Grok();
    private static final Logger _logger = LoggerFactory.getLogger(GrokTransform.class);
    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        String pfile = ctx.getString("pattern-file");
        if(pfile != null){
            _grok.addPatternFromFile(pfile);
        }
        else{
            Reader reader = new InputStreamReader(getClass().getResourceAsStream("/patterns"));
            _grok.addPatternFromReader(reader);
        }
        String pattern = ctx.optString("pattern", "%{COMMONAPACHELOG}");
        _logger.debug("Using pattern {}", pattern);
        _grok.compile(pattern);
    }

    @Override
    public void apply(VDSEvent inputEvent, VDSEventList outEvents) throws Exception {
        String input = new String(inputEvent.getBuffer().array(), 0, inputEvent.getBufferLen());
        String output = convertToJSON(input);
        if(output != null){
            _logger.debug("Grok'ed JSON output: {}", output);
            outEvents.addEvent(output.getBytes(), output.getBytes().length);
        }
        else{
            _logger.debug("Unmatched input line: {}", input);
        }

    }

    String convertToJSON(String input){
        Match match = _grok.match(input);
        match.captures();
        String output = match.toJson();
        return output;
    }

    @Override
    public void close() throws IOException {

    }
}
