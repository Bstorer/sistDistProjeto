package com.ufabc.mcta025.zkprimitives;

import java.io.IOException;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public abstract class ZKPrimitive implements Watcher {
    protected static ZooKeeper zk = null;
    protected static Object mutex;

    public ZKPrimitive(String address) {
        if (zk != null) {
            return;
        }
        try {
            System.out.println("Connecting to ZK...");
            zk = new ZooKeeper(address, 3000, this);
            mutex = new Object();
        } catch (IOException e) {
            e.printStackTrace();
            zk = null;
        }
    }
}
