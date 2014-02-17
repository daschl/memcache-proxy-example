package com.couchbase.proxy;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;

import java.net.InetSocketAddress;
import java.util.Arrays;

public class MemcacheProxyCient {

    public static void main(String[] args) throws Exception {
        ConnectionFactory cf = new BinaryConnectionFactory();
        MemcachedClient client = new MemcachedClient(cf, Arrays.asList(new InetSocketAddress(11212)));

        System.out.println(client.get("key0"));

        client.shutdown();
    }
}
