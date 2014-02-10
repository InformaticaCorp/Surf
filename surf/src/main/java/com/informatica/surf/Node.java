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

import com.informatica.um.binge.api.impl.VDSEventImpl;
import com.informatica.um.binge.api.impl.VDSEventListImpl;
import com.informatica.um.binge.api.impl.VDSEventListPoolFactory;
import com.informatica.um.binge.api.impl.VDSEventPoolFactory;
import com.informatica.um.binge.api.impl.VDSMessageAckSource;
import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSSource;
import com.informatica.vds.api.VDSTarget;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jraj
 */
public class Node implements Runnable {
    
    private VDSSource _source;
    private VDSTarget _target;
    private VDSConfiguration _context;
    private static final Logger _logger = LoggerFactory.getLogger(Node.class);
    private volatile boolean _shutdown = false;
    private final VDSMessageAckSource _acksource;
    private final boolean _needsAck;
    private final GenericKeyedObjectPool <Integer, VDSEventListImpl>_eventListPool; 
    
    public Node(VDSSource source, VDSTarget target, VDSConfiguration ctx){
        _logger.info("Creating node...");
        _source = source;
        _target = target;
        _context = ctx;
        if(source instanceof VDSMessageAckSource){
            _acksource = (VDSMessageAckSource)source;
            _needsAck = true;
        }
        else{
            _acksource = null;
            _needsAck = false;
        }
        GenericKeyedObjectPool<Integer, VDSEventImpl> evtpool = new GenericKeyedObjectPool(new VDSEventPoolFactory());
        evtpool.setMaxActive(-1);
        evtpool.setMaxTotal(100);
        _eventListPool = new GenericKeyedObjectPool<>(new VDSEventListPoolFactory(evtpool));
        _eventListPool.setMaxActive(-1);
        _eventListPool.setMaxTotal(100);
    }
    
    public void open() throws Exception{
        _logger.info("Opening source and target connections...");
        _source.open(_context);
        _target.open(_context);
    }
    
    @Override
    public void run(){
        _logger.info("Node run starting...");
        VDSEventListImpl events = null;
        while(!_shutdown){
            try{
                events = _eventListPool.borrowObject(0);
                _source.read(events);
                if(_needsAck){
                    Object obj = _acksource.getInputObject();
                    // Hacking this in for now
                    // Need to actually ack the object after Kinesis consumes it: TODO
                    _acksource.updateInputObject(obj);
                }
                processEvents(events);
            }
            catch(Exception ex){
                _logger.error("Exception while reading data:", ex);
                ex.printStackTrace();
            }
            finally{
                try{
                    _eventListPool.returnObject(0, events);
                }
                catch(Exception ex){
                    _logger.error("Exception while returning objects to the pool", ex);
                }
            }
        }
    }
    
    public void shutdown(){
        _shutdown = true;
    }
    private void processEvents(VDSEventListImpl events){
        for(VDSEventImpl evt: events.getEventsList()){
            try{
                _target.write(evt);
            }
            catch(Exception ex){
                _logger.error("Exception while writing to target", ex);
            }
        }
    }
    /*
    class WriteThread extends Thread{
        private final VDSEventListImpl _events;
        public WriteThread(VDSEventListImpl events){
            _logger.info("Write thread created");
            _events = events;
        }
        @Override
        public void run(){
            _logger.info("Write thread starting...");
            while(!_shutdown){
                List<VDSEventImpl> list = _events.getEventsList();
                while(!list.isEmpty()){
                    VDSEventImpl evt = list.remove(0);
                        
                    _logger.debug("WriteThread got an event, sendint to target");
                    try{
                        _target.write(evt);
                        _eventPool.returnObject(0, evt);
                    }
                    catch(Exception ex){
                        _logger.error("Exception while writing data:", ex);
                    }
                }
                try{sleep(10);}catch(Exception ex){}

            }
        }
    }
    */
    
}
