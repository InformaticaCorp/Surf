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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Informatica Corp
 */
public class HttpListener {
    private static final Logger _logger = LoggerFactory.getLogger(HttpListener.class);
    
    final private int _port;
    private Channel _channel;
    private final List<String> _ppaths; // POST paths
    private final List<String> _wpaths; // WebSocket paths;
    private final LinkedBlockingQueue<ByteBuf> _messageQueue = new LinkedBlockingQueue<>(); // TODO: make this bounded
    final EventLoopGroup _parentGroup = new NioEventLoopGroup();
    final EventLoopGroup _childGroup = new NioEventLoopGroup();
    
    public HttpListener(int port, List<String> ppaths, List<String>wpaths){
        _port = port;
        _ppaths = ppaths;
        _wpaths = wpaths;
        _logger.info("Created HttpListener on port {}", port);
    }
    
    public void start() throws InterruptedException{
        ServerBootstrap bs = new ServerBootstrap();
        bs.group(_parentGroup, _childGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new HttpServerInitializer(_ppaths, _wpaths, _messageQueue));
        _channel = bs.bind(_port).sync().channel();
        _logger.info("Started HttpListener listening on port {}", _port);
    }
    
    public void stop(){
        try{
            _channel.close();
            _channel.closeFuture().sync();
        }
        catch(InterruptedException ex){
            _logger.warn("Exception while waiting for HttpListener to shutdown", ex);
        }
        finally{
            _parentGroup.shutdownGracefully();
            _childGroup.shutdownGracefully();
        }
    }
    
    public byte []nextMessage() throws InterruptedException{
        _logger.debug("Checking queued messages");
        ByteBuf buf = _messageQueue.take();
        byte b[] = new byte[buf.readableBytes()];
        _logger.debug("Readable bytes = {}", b.length);
        buf.readBytes(b);
        buf.release();
        
        return b;
    }
}
