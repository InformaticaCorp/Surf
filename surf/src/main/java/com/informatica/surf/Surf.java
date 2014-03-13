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
package com.informatica.surf;

import com.informatica.surf.sources.kinesis.KinesisSource;
import com.informatica.surf.target.kinesis.KinesisTarget;
import com.informatica.vds.api.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


public class Surf
{
    private static final Logger _logger = LoggerFactory.getLogger(Surf.class);
    private static Map<String, Object> parseYaml(File file) throws IOException{
        Yaml yml = new Yaml();
        FileReader reader = new FileReader(file);
        Map<String, Object> conf = (Map<String, Object>)yml.load(reader);
        return conf;
    }
    public static void main( String[] args ) throws Exception
    {
        _logger.info("Surf node starting...");
        if(args.length != 1){
            usage();
            System.exit(1);
        }
        File file = new File(args[0]);
        if(!file.exists() || !file.canRead()){
            System.err.println("Cannot read file " + file);
            usage();
            System.exit(1);
        }
        boolean ingestMode = true;
        Map<Object, VDSConfiguration> contexts = new HashMap<>();
        Map<String, Object> conf = parseYaml(file);
        Map<String, Object> globalConfig = (Map)conf.get("configuration");
        Map<String, Object> srcMap = (Map)conf.get("source");
        Map<String, Object> tgtMap = (Map)conf.get("target");

        String nodeType = ((String)conf.get("node-type"));
        if("ingest".equals(nodeType)){
            ingestMode = true;
            _logger.info("Node running in ingest mode");
        }
        else if("process".equals(nodeType)){
            ingestMode = false;
            _logger.info("Node running in process mode");
        }
        else{
            throw new IllegalArgumentException("node-type must be 'process' or 'ingest'");
        }
        VDSSource src;
        VDSTarget tgt;
        if(ingestMode){
            // Ingest mode: the source is determined from configuration
            String srcClass = (String)srcMap.get("class");
            src = (VDSSource)Class.forName(srcClass).newInstance();
            Context srcCtx = new Context();
            srcCtx.setFromMap(globalConfig);
            srcCtx.setFromMap((Map<String, Object>) srcMap.get("configuration"));
            contexts.put(src, srcCtx);
            // The target is always Kinesis
            tgt = new KinesisTarget();
            Context tgtCtx = new Context();
            tgtCtx.setFromMap(globalConfig);
            contexts.put(tgt, tgtCtx);
        }
        else{
            // Process mode: the source is always Kinesis
            src = new KinesisSource();
            Context srcCtx = new Context();
            srcCtx.setFromMap(globalConfig);
            contexts.put(src, srcCtx);
            // The target is determined from configuration
            String tgtClass = (String)tgtMap.get("class");
            tgt = (VDSTarget)Class.forName(tgtClass).newInstance();
            Context tgtCtx = new Context();
            tgtCtx.setFromMap(globalConfig);
            tgtCtx.setFromMap((Map)tgtMap.get("configuration"));
            contexts.put(tgt, tgtCtx);
        }

        List<VDSTransform> transforms = new ArrayList<>();
        List <Map>txList = (List<Map>)conf.get("transforms");
        for(Map<String, Object> txMap: txList){
            String className = (String)txMap.get("class");
            Class clazz = Class.forName(className);
            Object txObj = clazz.newInstance();
            Map<String, Object> map = (Map)txMap.get("configuration");
            Context ctx = new Context();
            ctx.setFromMap(globalConfig);
            ctx.setFromMap(map);

            contexts.put(txObj, ctx);
            transforms.add((VDSTransform)txObj);
        }
        Node node = null;
        try{
            node = new Node(src, tgt, transforms, contexts);
            node.open();
            _logger.info("Surf Node opened");
            node.run();
        }
        catch(Exception ex){
            _logger.error("An exception occurred: ", ex);
            _logger.error("Shutting down node");
            node.shutdown();
            System.exit(-1);
        }
    }
    
    private static void usage(){
        System.err.println("Usage: Surf <config.yaml>");
    }
    
    static class LogOnlyTarget implements VDSTarget{

       private static final Logger _logger = LoggerFactory.getLogger(LogOnlyTarget.class);
       @Override
        public void open(VDSConfiguration ctx) throws Exception {
            _logger.info("Opened LogOnlyTarget");
        }


        @Override
        public void close() throws IOException {
            _logger.info("Closed LogOnlyTarget");
        }

        @Override
        public void write(VDSEvent strm) throws Exception {
            ByteBuffer buf = strm.getBuffer();
            byte []arr = new byte[strm.getBufferLen()];
            buf.get(arr);
            _logger.info("Received an event: {}", new String(arr));
        }
        
    }
}
