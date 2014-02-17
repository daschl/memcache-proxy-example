package com.couchbase.proxy;

import com.couchbase.client.CouchbaseClient;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.memcache.binary.AbstractBinaryMemcacheDecoder;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheObjectAggregator;
import io.netty.handler.codec.memcache.binary.BinaryMemcacheServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;

public class MemcacheProxyServer {

    private final int port;
    private final CouchbaseClient couchbaseClient;

    public MemcacheProxyServer(int port) {
        this.port = port;

        try {
            couchbaseClient = new CouchbaseClient(
                Arrays.asList(URI.create("http://127.0.0.1:8091/pools")),
                "default",
                ""
            );
        } catch (IOException ex) {
            throw new RuntimeException("Could not boot underlying Couchbase Client");
        }
    }

    public void start() throws Exception {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(eventLoopGroup)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(new InetSocketAddress(port))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel sc) throws Exception {
                            sc.pipeline()
                                .addLast(new LoggingHandler(LogLevel.INFO))
                                .addLast(new BinaryMemcacheServerCodec())
                                .addLast(new BinaryMemcacheObjectAggregator(AbstractBinaryMemcacheDecoder.DEFAULT_MAX_CHUNK_SIZE))
                                .addLast(new MemcacheProxyHandler(couchbaseClient));
                        }
                    });
            ChannelFuture channelFuture = serverBootstrap.bind().sync();
            System.out.println(MemcacheProxyServer.class.getName() +
                    " started on port " + channelFuture.channel().localAddress());
            channelFuture.channel().closeFuture().sync();
        } finally {
            eventLoopGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: " + MemcacheProxyServer.class.getSimpleName()
                    + " <port>");
            System.exit(0);
        }
        int port = Integer.parseInt(args[0]);
        MemcacheProxyServer server = new MemcacheProxyServer(port);
        server.start();
    }
}
