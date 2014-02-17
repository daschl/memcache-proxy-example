package com.couchbase.proxy;

import com.couchbase.client.CouchbaseClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheOpcodes;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequest;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheRequestHeader;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseHeader;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheResponseStatus;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheResponseHeader;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheResponse;
import io.netty.util.CharsetUtil;

public class MemcacheProxyHandler extends ChannelHandlerAdapter {

    private final CouchbaseClient client;

    public MemcacheProxyHandler(CouchbaseClient client) {
        this.client = client;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof BinaryMemcacheRequest) {
            BinaryMemcacheRequest request = (BinaryMemcacheRequest) msg;

            switch(request.getHeader().getOpcode()) {
                case BinaryMemcacheOpcodes.GET:
                    handleGet(ctx, request);
                    break;
                default:
                    throw new IllegalStateException("Got a opcode I don't understand: "
                            + request.getHeader().getOpcode());
            }
        } else {
            throw new IllegalStateException("Got a message I don't understand: " + msg);
        }
    }

    private void handleGet(ChannelHandlerContext ctx, BinaryMemcacheRequest request) {
        BinaryMemcacheRequestHeader requestHeader = request.getHeader();

        String key = request.getKey();

        Object couchbaseResponse = client.get(key);
        if (couchbaseResponse == null) {
            // TODO: send a ENOENT here back, since the doc was not found in couchbase
        }

        ByteBuf content = encodeResponseContent(couchbaseResponse);
        ByteBuf extras = flagsForObject(couchbaseResponse);

        BinaryMemcacheResponseHeader responseHeader = new DefaultBinaryMemcacheResponseHeader();
        responseHeader.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
        responseHeader.setOpcode(BinaryMemcacheOpcodes.GET);
        responseHeader.setKeyLength(requestHeader.getKeyLength());
        responseHeader.setOpaque(requestHeader.getOpaque());
        responseHeader.setExtrasLength((byte) extras.readableBytes());
        responseHeader.setTotalBodyLength(content.readableBytes() + extras.readableBytes());

        BinaryMemcacheResponse response = new DefaultFullBinaryMemcacheResponse(responseHeader, "",
            extras, content);
        ctx.writeAndFlush(response);
    }

    private ByteBuf encodeResponseContent(Object input) {
        // Needs to be fixed with a nice transcoder depending on the object input.
        return Unpooled.copiedBuffer("Hello World", CharsetUtil.UTF_8);
    }

    private ByteBuf flagsForObject(Object response) {
        if (response instanceof String) {
            return  Unpooled.buffer().writeInt(0); // 0 flag == string
        } else {
            throw new IllegalStateException("Got Couchbase response I don't know the flags of: "
                + response.getClass().getSimpleName());
        }
    }
}
