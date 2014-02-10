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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import java.util.List;
import java.util.Queue;

/**
 *
 * @author Informatica Corp
 */
public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {
    private final List<String> _ppaths; // POST paths
    private final List<String> _wpaths; // WebSocket paths;
    private final Queue<ByteBuf> _messageQueue;
    public HttpServerInitializer(List<String> ppaths, List<String>wpaths, Queue<ByteBuf> mqueue){
        _ppaths = ppaths;
        _wpaths = wpaths;
        _messageQueue = mqueue;
    }
    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("codec-http", new HttpServerCodec());
        pipeline.addLast("aggregator", new HttpObjectAggregator(65536)); // TODO: make this same as block size
        pipeline.addLast("handler", new HttpServerHandler(_ppaths, _wpaths, _messageQueue));
    }
}