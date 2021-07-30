package com.ufabc.mcta025.zkprimitives;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.ufabc.mcta025.zktools.ZKOperator;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

public class ZKLock extends ZKPrimitive {
    private long wait;
    private String root;
	String pathName;
    String name;
    
    public ZKLock(String address, String root, long waitTime) throws KeeperException, InterruptedException, UnknownHostException {
        super(address);
        this.root = root;
        this.wait = waitTime;

        ZKOperator.safeCreate(zk, root, new byte[0], CreateMode.PERSISTENT);
        name = this.root + "/" + new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
    }

    public void lock() throws KeeperException, InterruptedException{
        name = ZKOperator.create(zk, name, new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
        compute();
    }

    void compute() {
        System.out.println("[LOCK] Locked - wait for " + wait/1000 + " seconds");
        try {
            new Thread();
            Thread.sleep(wait);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //Exits, which releases the ephemeral node (Unlock operation)
        System.out.println("[LOCK] Free");
    }

    @Override
    public void process(WatchedEvent event) {
    }
}
