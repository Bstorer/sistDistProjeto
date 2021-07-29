package com.ufabc.mcta025.zkprimitives;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.ufabc.mcta025.zktools.ZKOperator;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

public class ZKBarrier extends ZKPrimitive {
    private int size;
    private String root;
    private String name;
    public boolean barrierIsFull;

    public ZKBarrier(String address, String root, int size) {
        super(address);
        this.root = root;
        this.size = size;
        this.barrierIsFull = false;
    }

    public boolean initialize() throws KeeperException, InterruptedException, UnknownHostException {
        String path = ZKOperator.safeCreate(zk, root, new byte[0], CreateMode.PERSISTENT);
        name = root + "/" + new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
        return path != root;
    }

    public boolean enter() throws KeeperException, InterruptedException {
        if (ZKOperator.getChildrenCount(zk, root, false) < size) {
            name = ZKOperator.create(zk, name, new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
            return true;
        } else {
            return false;
        }
    }

    public void checkBarrier() throws KeeperException, InterruptedException {
        barrierIsFull = ZKOperator.getChildrenCount(zk, root, false) >= size;
    }

    public void leave() throws InterruptedException, KeeperException {
        ZKOperator.delete(zk, name);
    }

    @Override
    public void process(WatchedEvent event) {
    }
}
