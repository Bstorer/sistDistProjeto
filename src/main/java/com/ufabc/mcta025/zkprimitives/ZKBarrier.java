package com.ufabc.mcta025.zkprimitives;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.ufabc.mcta025.zktools.ZKOperator;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

public class ZKBarrier extends ZKPrimitive {

    public final class BarrierState {
        public final static int WAIT = 0;
        public final static int RELEASE = 1;
    }

    private int size;
    private String root;
    private String name;
    private ZKState zkStateBarrier;

    public ZKBarrier(String address, String root, int size) {
        super(address);
        this.root = root;
        this.size = size;
        zkStateBarrier = new ZKState(address, root);
    }

    public boolean initialize() throws KeeperException, InterruptedException, UnknownHostException {
        zkStateBarrier.initialize();
        zkStateBarrier.setState(BarrierState.WAIT);
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

    synchronized public boolean isFull() throws KeeperException, InterruptedException {
        return ZKOperator.getChildrenCount(zk, root, false) >= size;
    }

    public void leave() throws InterruptedException, KeeperException {
        ZKOperator.delete(zk, name);
    }

    public boolean hasReleaseState() throws KeeperException, InterruptedException {
        return zkStateBarrier.getState() == BarrierState.RELEASE;
    }

    public void updateReleaseState() throws KeeperException, InterruptedException {
        if (isFull()) {
            zkStateBarrier.setState(BarrierState.RELEASE);
        }
    }

    @Override
    public void process(WatchedEvent event) {
    }
}
