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
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheResponse;
import io.netty.handler.codec.memcache.binary.DefaultBinaryMemcacheResponseHeader;
import io.netty.handler.codec.memcache.binary.DefaultFullBinaryMemcacheResponse;
import io.netty.util.CharsetUtil;
import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

public class MemcacheProxyHandler extends ChannelHandlerAdapter {

    private final CouchbaseClient client;
    private final Transcoder transcoder;

    public MemcacheProxyHandler(CouchbaseClient client) {
        this.client = client;
        transcoder = new PassthroughTranscoder();
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

        BinaryMemcacheResponseHeader responseHeader = new DefaultBinaryMemcacheResponseHeader();
        responseHeader.setOpcode(BinaryMemcacheOpcodes.GET);
        responseHeader.setOpaque(requestHeader.getOpaque());

        CachedData couchbaseResponse = (CachedData) client.get(key, transcoder);
        if (couchbaseResponse == null) {
            responseHeader.setStatus(BinaryMemcacheResponseStatus.KEY_ENOENT);
            ctx.writeAndFlush(
                new DefaultBinaryMemcacheResponse(responseHeader)
            );
        } else {
            ByteBuf content = Unpooled.copiedBuffer(couchbaseResponse.getData());
            ByteBuf extras = Unpooled.buffer().writeInt(couchbaseResponse.getFlags());

            responseHeader.setStatus(BinaryMemcacheResponseStatus.SUCCESS);
            responseHeader.setExtrasLength((byte) extras.readableBytes());
            responseHeader.setTotalBodyLength(content.readableBytes() + extras.readableBytes());

            ctx.writeAndFlush(
                new DefaultFullBinaryMemcacheResponse(responseHeader, "",extras, content)
            );
        }
    }


    static class PassthroughTranscoder extends SerializingTranscoder {
        @Override
        public Object decode(CachedData d) {
            return d;
        }
    }
}
