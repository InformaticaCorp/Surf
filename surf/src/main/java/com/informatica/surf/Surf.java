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
    public static final String VDS_SOURCE_CLASS = "vds-source-class";
    public static final String LOG_TARGET_ONLY="surf-log-target-only";
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
        Map<Object, VDSConfiguration> contexts = new HashMap<>();
        Map<String, Object> conf = parseYaml(file);
        Map<String, Object> globalConfig = (Map<String, Object>)conf.get("configuration");
        Map<String, Object> ingest = (Map<String, Object>)conf.get("ingest");
        Map<String, Object> srcMap = (Map<String, Object>)ingest.get("source");

        String srcClass = (String)srcMap.get("class");
        VDSSource src = (VDSSource)Class.forName(srcClass).newInstance();
        Context srcCtx = new Context();
        srcCtx.setFromMap(globalConfig);
        srcCtx.setFromMap((Map<String, Object>) srcMap.get("configuration"));
        contexts.put(src, srcCtx);

        VDSTarget tgt;
        tgt = new KinesisTarget();
        Context tgtCtx = new Context();
        tgtCtx.setFromMap(globalConfig);
        contexts.put(tgt, tgtCtx);

        List<VDSTransform> transforms = new ArrayList<>();
        List <Map>txList = (List<Map>)ingest.get("transforms");
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

        Node node = new Node(src, tgt, transforms, contexts);
        node.open();
        node.run();
    }
    
    private static void usage(){
        System.err.println("Usage: Surf <config.properties>");
        System.err.println("config.properties *must* contain at least the following properties:");
        System.err.printf("%s: the AWS Access Key ID for your Kinesis account\n", KinesisTarget.ACCESS_KEY);
        System.err.printf("%s: the AWS Secret Key for the above ID\n", KinesisTarget.SECRET_KEY);
        System.err.printf("%s: the Kinesis stream name where data should be published\n", KinesisTarget.STREAM_NAME);
        System.err.printf("%s: the fully-qualified classname of the VDS source\n", VDS_SOURCE_CLASS);
        System.err.println("\n\n");
        System.err.println("In addition, you must specify any configuration parameters required by the source class");
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
