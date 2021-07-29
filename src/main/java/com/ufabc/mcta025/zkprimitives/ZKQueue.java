package com.ufabc.mcta025.zkprimitives;

import java.nio.ByteBuffer;

import com.ufabc.mcta025.zktools.ZKOperator;
import com.ufabc.mcta025.zktools.ZKUtils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

public class ZKQueue extends ZKPrimitive {

    private String root;

    public ZKQueue(String address, String name) {
        super(address);
        this.root = name;
    }

    public boolean initialize() throws KeeperException, InterruptedException {
        String path = ZKOperator.safeCreate(zk, root, new byte[0], CreateMode.PERSISTENT);
        return path != root;
    }

    public void produce(int i) throws KeeperException, InterruptedException {
        ByteBuffer b = ByteBuffer.allocate(4);
        byte[] value;

        b.putInt(i);
        value = b.array();
        ZKOperator.create(zk, root + "/element", value, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    public int consume() throws KeeperException, InterruptedException {
        int retValue = -1;
        int min = ZKOperator.getMinSequential(zk, root);
        String nodePath = root + "/element" + ZKUtils.intToSequence(min);
        byte[] b = ZKOperator.safeGetData(zk, nodePath);
        ZKOperator.delete(zk, nodePath);
        ByteBuffer buffer = ByteBuffer.wrap(b);
        retValue = buffer.getInt();
        return retValue;
    }

    public boolean isEmpty() throws KeeperException, InterruptedException {
        return ZKOperator.getChildrenCount(zk, root, false) == 0;
    }

    @Override
    public void process(WatchedEvent event) {

    }

}
