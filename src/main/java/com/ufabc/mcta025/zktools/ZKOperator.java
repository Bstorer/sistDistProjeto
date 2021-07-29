package com.ufabc.mcta025.zktools;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZKOperator {

    public static String create(ZooKeeper zk, String path, byte[] data, CreateMode creationMode)
            throws KeeperException, InterruptedException {
        return zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, creationMode);
    }

    public static String safeCreate(ZooKeeper zk, String path, byte[] data, CreateMode creationMode)
            throws KeeperException, InterruptedException {
        if (!exists(zk, path)) {
            return zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, creationMode);
        } else {
            return path;
        }
    }

    public static void delete(ZooKeeper zk, String path) throws InterruptedException, KeeperException {
        zk.delete(path, -1);
    }

    public static int getChildrenCount(ZooKeeper zk, String path, boolean watch)
            throws KeeperException, InterruptedException {
        return zk.getChildren(path, watch).size();
    }

    public static boolean exists(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        return zk.exists(path, false) != null;
    }

    public static byte[] safeGetData(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        Stat s = zk.exists(path, false);
        if (s != null) {
            return zk.getData(path, false, s);
        } else {
            return "Node not found!".getBytes();
        }
    }

    public static int getMinSequential(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        List<String> nodeList = zk.getChildren(path, false);
        String firstNode = nodeList.get(0);
        int length = firstNode.length();
        int min = new Integer(firstNode.substring(length - 10, length));
        for (String node : nodeList) {
            length = node.length();
            int sequence = new Integer(node.substring(length - 10, length));
            if (sequence < min) {
                min = sequence;
            }
        }
        return min;
    }

    public static int getPrevSequential(ZooKeeper zk, String path, int min, int sequence)
            throws KeeperException, InterruptedException {
        List<String> nodeList = zk.getChildren(path, false);
        int prev = sequence;
        for (String node : nodeList) {
            int length = node.length();
            int currSequence = new Integer(node.substring(length - 10, length));
            if (currSequence > min && currSequence < sequence) {
                prev = currSequence;
            }
        }
        return prev;
    }

    public static boolean safeAddWatcher(ZooKeeper zk, String path, Watcher watcher)
            throws KeeperException, InterruptedException {
        if (exists(zk, path)) {
            zk.exists(path, watcher);
            return true;
        } else {
            return false;
        }
    }

    public static void safeWrite(ZooKeeper zk, String path, byte[] data, CreateMode creationMode)
            throws KeeperException, InterruptedException {
        if (!exists(zk, path)) {
            create(zk, path, data, creationMode);
        }
        zk.setData(path, data, -1);
    }
}
