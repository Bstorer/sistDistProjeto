package com.ufabc.mcta025;

import com.ufabc.mcta025.leilao.AuctionManager;
import com.ufabc.mcta025.leilao.LeaderManager;
import com.ufabc.mcta025.leilao.LobbyManager;
import com.ufabc.mcta025.leilao.AuctionManager.AuctionState;

import org.apache.zookeeper.KeeperException;

public final class App {
    public static final int V_MAJOR = 0;
    public static final int V_MINOR = 0;
    public static final int V_PATCH = 1;

    public static void main(String[] args) {
        final String HOST = "localhost";
        final String PATH_LOBBY = "/lobby";
        final String PATH_ELECTION = "/election";
        final String PATH_LEADER = "/leader";
        final String PATH_AUCTION = "/auction";
        final String PATH_BID_QUEUE = "/bid_queue";
        final String PATH_MAX_BID = "/max_bid";
        final int MIN_CLIENTS = 3;
        final int END_TIME = 50;
        final String version = String.format("%d.%d.%d", V_MAJOR, V_MINOR, V_PATCH);

        System.out.println("[Distributed Auction] v" + version);

        try {
            LeaderManager leaderManager = new LeaderManager(HOST, PATH_ELECTION, PATH_LEADER);
            LobbyManager lobbyManager = new LobbyManager(HOST, PATH_LOBBY, MIN_CLIENTS);
            AuctionManager auctionManager = new AuctionManager(HOST, PATH_AUCTION, PATH_BID_QUEUE, PATH_MAX_BID,
                    END_TIME);
            leaderManager.start();
            lobbyManager.start();
            lobbyManager.join();
            auctionManager.start();
            while (auctionManager.auctionState != AuctionState.ENDED) {
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
