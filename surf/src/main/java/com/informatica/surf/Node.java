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
import com.informatica.vds.api.*;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 *
 * @author jraj
 */
public class Node implements Runnable, EventHandler<SurfEvent> {
    
    private VDSSource _source;
    private VDSTarget _target;
    private VDSConfiguration _context;
    private static final Logger _logger = LoggerFactory.getLogger(Node.class);
    private volatile boolean _shutdown = false;
    private final VDSMessageAckSource _acksource;
    private final boolean _needsAck;
    private final GenericKeyedObjectPool <Integer, VDSEventListImpl>_eventListPool;
    private final List<VDSTransform> _transforms;
    
    public Node(VDSSource source, VDSTarget target, List<VDSTransform> transforms, VDSConfiguration ctx){
        _logger.info("Creating node...");
        _source = source;
        _target = target;
        _transforms = transforms;
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
        Executor executor = Executors.newCachedThreadPool();
        Disruptor<SurfEvent> disruptor = new Disruptor<>(SurfEvent.EVENT_FACTORY, 128, executor);
        disruptor.handleEventsWith(this);
        RingBuffer<SurfEvent> buffer = disruptor.start();


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
                long seq = buffer.next();
                SurfEvent event = buffer.get(seq);
                event.setEventlist(events);
                buffer.publish(seq);
            }
            catch(Exception ex){
                _logger.error("Exception while reading data:", ex);
                ex.printStackTrace();
            }
        }
    }
    
    public void shutdown(){
        _shutdown = true;
    }

    @Override
    public void onEvent(SurfEvent surfEvent, long l, boolean b) throws Exception {
        VDSEventListImpl srcEvents = surfEvent.getEventlist();
        VDSEventListImpl txEvents = applyTransforms(srcEvents, _transforms.iterator());
        for(VDSEventImpl evt: txEvents.getEventsList()){
            _target.write(evt);
        }
        _eventListPool.returnObject(0, txEvents);
    }

    private VDSEventListImpl applyTransforms(VDSEventListImpl srcEvents, Iterator<VDSTransform> transforms) throws Exception{
        if(!transforms.hasNext()){
            return srcEvents;
        }
        VDSTransform tx = transforms.next();
        VDSEventListImpl outlist = _eventListPool.borrowObject(0);
        for(VDSEventImpl evt: srcEvents.getEventsList()){
            tx.apply(evt, outlist);
        }
        _eventListPool.returnObject(0, srcEvents);
        return applyTransforms(outlist, transforms);

    }

}
