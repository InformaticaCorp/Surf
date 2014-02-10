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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import static io.netty.handler.codec.http.HttpHeaders.Names.HOST;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.setContentLength;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.util.CharsetUtil;
import java.util.List;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Informatica Corp
 */
class HttpServerHandler extends SimpleChannelInboundHandler<Object> {

    private static final Logger _logger = LoggerFactory.getLogger(HttpServerHandler.class);
    private final List<String> _ppaths; // POST paths
    private final List<String> _wpaths; // WebSocket paths;
    private final Queue<ByteBuf> _messageQueue;
    private WebSocketServerHandshaker _handshaker;

    public HttpServerHandler(List<String>ppaths, List<String>wpaths, Queue<ByteBuf> mqueue) {
        _ppaths = ppaths;
        _wpaths = wpaths;
        _messageQueue = mqueue;
        _logger.info("Initialized HttpHandler.");
        _logger.info("POST paths:");
        for(String s: ppaths){
            _logger.info(s);
        }
        _logger.info("WebSocket paths:");
        for(String s: wpaths){
            _logger.info(s);
        }
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FullHttpRequest) {
            handleHttpRequest(ctx, (FullHttpRequest) msg);
        } else if (msg instanceof WebSocketFrame) {
            handleWebSocketFrame(ctx, (WebSocketFrame) msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {
        _logger.debug("Got a http request");
        // Handle a bad request.
        if (!req.getDecoderResult().isSuccess()) {
            _logger.debug("Could not decode request");
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST));
            return;
        }

        boolean handled = false;
        
        if (req.getMethod() == GET) {
            _logger.debug("Got a GET request");
            String path = req.getUri();
            if(_wpaths.contains(path)){
                handleWebSocketHandshake(ctx, req);
                handled = true;
            }
            else{
                _logger.debug("Unknown path {}", path);
            }
        }
        
        // Handle POST method
        if(req.getMethod() == POST){
            _logger.debug("Got a POST request");
            // Check if the URL is present in _ppaths. This means it is a normal POST request
            String path = req.getUri();
            if(_ppaths.contains(path)){
                _logger.debug("Path matches POST path list");
                handlePost(ctx, req);
                handled = true;
            }
        }
        if(!handled){
            _logger.debug("Could not handle request, returning error");
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST));
        }
    }
    
    private void handlePost(ChannelHandlerContext ctx, FullHttpRequest req){
        _messageQueue.add(req.content().retain());
        _logger.debug("Added POSTed message to queue");
        sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, OK));
        
    }
    
    private void handleWebSocketHandshake(ChannelHandlerContext ctx, FullHttpRequest req){
        String location = "ws://" + req.headers().get(HOST) + req.getUri();

        WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
                location, null, false);
        _handshaker = wsFactory.newHandshaker(req);
        if (_handshaker == null) {
            WebSocketServerHandshakerFactory.sendUnsupportedWebSocketVersionResponse(ctx.channel());
        } else {
            _handshaker.handshake(ctx.channel(), req);
        }
        
    }
    

    private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
        _logger.debug("Handling websocket frame");
        // Check for closing frame
        if (frame instanceof CloseWebSocketFrame) {
            _handshaker.close(ctx.channel(), (CloseWebSocketFrame) frame.retain());
            return;
        }
        if (frame instanceof PingWebSocketFrame) {
            ctx.channel().write(new PongWebSocketFrame(frame.content().retain()));
            return;
        }
        if (!(frame instanceof TextWebSocketFrame)) {
            throw new UnsupportedOperationException(String.format("%s frame types not supported", frame.getClass()
                    .getName()));
        }

        String request = ((TextWebSocketFrame) frame).text();
        _logger.debug("{} received {}", ctx.channel(), request);
        _messageQueue.add(frame.content().retain());
        //ctx.channel().write(new TextWebSocketFrame(request.toUpperCase()));
    }

    private static void sendHttpResponse(
            ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse res) {
        // Generate an error page if response getStatus code is not OK (200).
        if (res.getStatus().code() != 200) {
            ByteBuf buf = Unpooled.copiedBuffer(res.getStatus().toString(), CharsetUtil.UTF_8);
            res.content().writeBytes(buf);
            buf.release();
            setContentLength(res, res.content().readableBytes());
        }
        else{
            setContentLength(res, 0);
        }

        // Send the response and close the connection if necessary.
        ChannelFuture f = ctx.channel().writeAndFlush(res);
        if (!isKeepAlive(req) || res.getStatus().code() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        _logger.warn("Exception while handing HTTP request", cause);
        ctx.close();
    }
}
