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

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.lmax.disruptor.EventHandler;
import java.io.File;

public class PageCount 
{
    private static void errorExit(){
        System.out.println("Please provide a property file name. The file should contain:");
        System.out.println("aws-access-key-id: <your-access-id>");
        System.out.println("aws-secret-key: <your-secret-key>");
        System.out.println("aws-kinesis-stream-name: <your-stream-name>");
        System.out.println("You can provide the same property file as your Surf node");
        System.exit(1);
    }
    public static void main( String[] args ) throws Exception
    {
        // args[0] is expected to be the same sort of property file as needed
        // by Surf for Kinesis: it should contain 
        // aws-access-key-id: <your-access-id>
        // aws-secret-key: <your-secret-key>
        // aws-kinesis-stream-name: <your-stream-name>
        if (args.length != 1){
            errorExit();
        }
        File f = new File(args[0]);
        if(!f.isFile() || !f.canRead()){
            errorExit();
        }

        // Set up disruptor
        final EventHandler<KinesisEvent> handler = new PageCountHandler();
        Worker worker = Util.createWorker(f, handler, "PageCount");
        worker.run();
    }        
}
