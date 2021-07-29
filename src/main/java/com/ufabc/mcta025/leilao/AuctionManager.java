package com.ufabc.mcta025.leilao;

import java.util.Scanner;

import com.ufabc.mcta025.zkprimitives.ZKQueue;
import com.ufabc.mcta025.zkprimitives.ZKState;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class AuctionManager extends AuthoritativeManager implements Watcher {

    public final class AuctionState {
        public static final int WAITING = 0;
        public static final int RUNNING = 1;
        public static final int ENDED = 2;
    }

    public int auctionState;
    private int currentBid = -1;
    private int retValue = -1;
    private int endTime;
    private ZKQueue zkQueue;
    private ZKState zkStateAuction;
    private ZKState zkStateBid;

    public AuctionManager(String address, String auctionPath, String bidQueuePath, String maxBidPath, int endTime)
            throws KeeperException, InterruptedException {
        this.zkStateAuction = new ZKState(address, auctionPath);
        this.zkStateBid = new ZKState(address, maxBidPath);
        this.zkQueue = new ZKQueue(address, bidQueuePath);
        this.endTime = endTime;
        setAuctionState(AuctionState.WAITING);
    }

    public void run() {
        try {
            System.out.println("[Auction Manager]: Starting auction.");
            zkStateAuction.initialize();
            zkStateBid.initialize();
            if (hasAuthorization) {
                zkStateBid.setState(0); // Reset initial maxBid
            }
            setAuctionState(AuctionState.RUNNING);
            System.out.println("[Auction Manager]: Creating bid queue.");
            boolean bidQueueCreated = zkQueue.initialize();
            if (bidQueueCreated) {
                System.out.println("[Auction Manager]: Bid queue created.");
            } else {
                System.out.println("[Auction Manager]: Bid queue already exists.");
            }
            Scanner s = new Scanner(System.in);
            while (auctionState == AuctionState.RUNNING) {
                int maxBid = getMaxBid();
                if (maxBid != -1) {
                    System.out.println("[Auction Manager]: Current winning bid: [" + maxBid + "].");
                }
                System.out.println("[Auction Manager]: Enter your bid:");
                currentBid = s.nextInt();
                zkQueue.produce(currentBid);
                // if (!hasAuthorization) return;
                updateMaxBid();
                if (System.currentTimeMillis() - zkStateAuction.getStat().getMtime() >= endTime * 1000) {
                    System.out.println("[Auction Manager]: Since no bids have been placed in the last " + endTime
                            + " second(s), the auction will now end.");
                    setAuctionState(AuctionState.ENDED);
                    break;
                }
            }
            System.out.println("[Auction Manager]: End of auction.");
            setAuctionState(AuctionState.ENDED);
            s.close();
        } catch (KeeperException |

                InterruptedException e) {
            e.printStackTrace();
        }
    }

    public int getMaxBid() throws KeeperException, InterruptedException {
        return zkStateBid.getState();
    }

    public void updateMaxBid() throws KeeperException, InterruptedException {
        while (!zkQueue.isEmpty()) {
            int bid = zkQueue.consume();
            if (bid > getMaxBid()) {
                retValue = bid;
            }
        }
        this.zkStateBid.setState(retValue);
    }

    public void setAuctionState(int state) throws KeeperException, InterruptedException {
        auctionState = state;
        zkStateAuction.setState(state);
    }

    @Override
    public void process(WatchedEvent event) {
    }
}
