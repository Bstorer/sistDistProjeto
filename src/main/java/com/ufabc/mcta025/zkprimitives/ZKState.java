package com.ufabc.mcta025.zkprimitives;

import java.nio.ByteBuffer;

import com.ufabc.mcta025.zktools.ZKOperator;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

public class ZKState extends ZKPrimitive {

    private String root;

    public ZKState(String address, String name) {
        super(address);
        this.root = name;
    }

    public boolean initialize() throws KeeperException, InterruptedException {
        String path = ZKOperator.safeCreate(zk, root, new byte[0], CreateMode.PERSISTENT);
        return path != root;
    }

    public int getState() throws KeeperException, InterruptedException {
        return ByteBuffer.wrap(ZKOperator.safeGetData(zk, root)).getInt();
    }

    public Stat getStat() throws KeeperException, InterruptedException {
        Stat s = new Stat();
        zk.getData(root, this, s);
        return s;
    }

    public void setState(int state) throws KeeperException, InterruptedException {
        ByteBuffer b = ByteBuffer.allocate(4);
        ZKOperator.safeWrite(zk, root, b.putInt(state).array(), CreateMode.PERSISTENT);
    }

    @Override
    public void process(WatchedEvent event) {
    }
}
