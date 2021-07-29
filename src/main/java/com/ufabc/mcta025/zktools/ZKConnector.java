package com.ufabc.mcta025.zktools;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZKConnector {
    private ZooKeeper zk;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    public ZooKeeper connect(String host, Watcher watcher) throws IOException, InterruptedException {
        zk = new ZooKeeper(host, 3000, watcher);
        return zk;
    }

    public ZooKeeper connectWithLatch(String host, Watcher watcher) throws IOException, InterruptedException {
        zk = new ZooKeeper(host, 3000, watcher);
        countDownLatch.await();
        return zk;
    }

    public void close() throws InterruptedException {
        zk.close();
    }

}
