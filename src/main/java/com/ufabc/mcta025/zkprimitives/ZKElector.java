package com.ufabc.mcta025.zkprimitives;

import com.ufabc.mcta025.zktools.ZKOperator;
import com.ufabc.mcta025.zktools.ZKUtils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ZKElector extends ZKPrimitive {

    private String root;
    private String leader;
    private int sequence;
    private String id;
    public boolean isElegible;

    public ZKElector(String address, String root, String leader, String id) {
        super(address);
        this.root = root;
        this.leader = leader;
        this.id = id;
    }

    public boolean initialize() throws KeeperException, InterruptedException {
        String path = ZKOperator.safeCreate(zk, root, new byte[0], CreateMode.PERSISTENT);
        return path != root;
    }

    public void apply() throws KeeperException, InterruptedException {
        String node = ZKOperator.safeCreate(zk, root + "/n_", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
        sequence = ZKUtils.sequenceToInt(node);
        updateWatcher();
    }

    public boolean updateWatcher() throws KeeperException, InterruptedException {
        int min = ZKOperator.getMinSequential(zk, root);
        int prev = ZKOperator.getPrevSequential(zk, root, min, sequence);
        return ZKOperator.safeAddWatcher(zk, root + "/" + ZKUtils.intToSequence(prev), this);
    }

    public void checkEligibility() throws KeeperException, InterruptedException {
        if (sequence == ZKOperator.getMinSequential(zk, root)) {
            isElegible = true;
        } else {
            isElegible = false;
        }
    }

    public void elect() throws KeeperException, InterruptedException {
        ZKOperator.safeWrite(zk, leader, id.getBytes(), CreateMode.EPHEMERAL);
    }

    @Override
    public synchronized void process(WatchedEvent wEvent) {
        synchronized (mutex) {
            if (wEvent.getType() == EventType.NodeDeleted) {
                try {
                    updateWatcher();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
