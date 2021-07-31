package com.ufabc.mcta025.leilao;

import java.net.UnknownHostException;

import com.ufabc.mcta025.zkprimitives.ZKBarrier;

import org.apache.zookeeper.KeeperException;

public class LobbyManager extends AuthoritativeManager {

    private ZKBarrier zkBarrier;

    public LobbyManager(String address, String lobbyPath, int minClients) {
        this.zkBarrier = new ZKBarrier(address, lobbyPath, minClients);
    }

    public void run() {
        try {
            System.out.println("[LOBBY MANAGER]: Creating lobby.");
            boolean created = zkBarrier.initialize();
            if (created) {
                System.out.println("[LOBBY MANAGER]: Succesfully created lobby.");
            } else {
                System.out.println("[LOBBY MANAGER]: Lobby already exists.");
            }
            System.out.println("[LOBBY MANAGER]: Joining the lobby.");
            boolean entered = zkBarrier.enter();
            if (entered) {
                System.out.println("[LOBBY MANAGER]: Joined the lobby.");
            } else {
                System.out.println("[LOBBY MANAGER]: Failed to join the lobby.");
            }
            synchronized (this) {
                System.out.println("[LOBBY MANAGER]: Waiting for others to join...");
                while (!zkBarrier.hasReleaseState()) {
                    if (hasAuthorization) {
                        zkBarrier.updateReleaseState();
                    }
                }
                System.out.println("[LOBBY MANAGER]: Lobby is full.");
            }
            System.out.println("[LOBBY MANAGER]: Leaving the lobby.");
            zkBarrier.leave();
            System.out.println("[LOBBY MANAGER]: Left the lobby.");
            System.out.println("[LOBBY MANAGER]: Ready to proceed.");
        } catch (UnknownHostException | KeeperException |

                InterruptedException e) {
            e.printStackTrace();
        }
    }
}
