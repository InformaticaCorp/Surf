package com.informatica.surf.transforms.regextojson;

import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEvent;
import com.informatica.vds.api.VDSEventList;
import com.informatica.vds.api.VDSTransform;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This transform converts text lines to JSON objects. It uses a customizable regex to do so.
 * Named groups in the regex will be converted to corresponding JSON keys, and the matched string
 * will be set as the value. It does not support complex JSON types like arrays, nested objects etc.
 * It needs two settings from configuration:
 * regex: this is the regular expression with named groups
 * groups: this a comma-separated list of group names. Java does not currently have a way to programatically determine this
 * The default regex matches the common Apache log format http://en.wikipedia.org/wiki/Common_Log_Format
 */
public class RegexToJSON implements VDSTransform {
    public static final String DEFAULT_REGEX =
            "(?<host>[^ ]*) (?<clientid>[^ ]*) (?<userid>[^ ]*) \\[(?<timestamp>.*)\\] \\\"(?<request>[^ ]*) " +
            "(?<url>[^ ]*) (?<version>[^ ]*)\\\" (?<status>[0-9]*) (?<size>[0-9\\-]*)";
    public static final String DEFAULT_GROUPS = "host, clientid, userid, timestamp, request, url, version, status, size";
    private Pattern _pattern;
    private String[]_groups;
    private static final Logger _logger = LoggerFactory.getLogger(RegexToJSON.class);
    private final SimpleDateFormat _sdf = new SimpleDateFormat("dd/MMM/YYYY:HH:mm:ss Z");

    //String d  = "13/Mar/2014:03:10:51 -0700"
    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        String regex = ctx.optString("regex", DEFAULT_REGEX);
        _pattern = Pattern.compile(regex);
        String strgrp = ctx.optString("groups", DEFAULT_GROUPS);
        StringTokenizer tok = new StringTokenizer(strgrp, ",");
        _groups = new String[tok.countTokens()];
        int i=0;
        while(tok.hasMoreTokens()){
            _groups[i] = tok.nextToken().trim();
            _logger.debug("Adding group {}", _groups[i]);
            i++;
        }
        _logger.info("RegexToJSON transform initialized with regex: {}", regex);
    }

    @Override
    public void apply(VDSEvent inputEvent, VDSEventList outEvents) throws Exception {
        String input = new String(inputEvent.getBuffer().array(), 0, inputEvent.getBufferLen());
        _logger.debug("Applying transform to input: {}", input);
        Matcher m = _pattern.matcher(input);
        if(m.matches()){
            JSONObject json = new JSONObject();
            for(String g: _groups){
                String val = m.group(g);
                if(val == null) {
                    continue;
                }
                if(g.equals("timestamp")){
                    g = "@timestamp";
                    Date v = _sdf.parse(val);
                    json.put(g, v.getTime());
                }
                else{
                    json.put(g, val);
                }
            }
            String strJSON = json.toJSONString(JSONStyle.LT_COMPRESS);
            byte []buf = strJSON.getBytes();
            _logger.debug("JSON output: {}", strJSON);
            outEvents.addEvent(buf, buf.length);
        }
        else{
            _logger.debug("Line not matched: {}", input);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
