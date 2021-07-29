package com.ufabc.mcta025.leilao;

import java.util.Random;

import com.ufabc.mcta025.zkprimitives.ZKElector;

import org.apache.zookeeper.KeeperException;

public class LeaderManager extends AuthoritativeManager {

    private ZKElector zkElector;

    public LeaderManager(String address, String electionPath, String leaderPath)
            throws KeeperException, InterruptedException {
        Integer id = new Random().nextInt(10000000);
        this.zkElector = new ZKElector(address, electionPath, leaderPath, id.toString());
    }

    public void run() {
        try {
            System.out.println("[LEADER MANAGER]: Creating election zone.");
            boolean created = zkElector.initialize();
            if (created) {
                System.out.println("[LEADER MANAGER]: Successfully created election zone.");
            } else {
                System.out.println("[LEADER MANAGER]: Election zone already exists.");
            }
            System.out.println("[LEADER MANAGER]: Applying for leader.");
            zkElector.apply();
            System.out.println("[LEADER MANAGER]: Succesfully applied for leader.");
            synchronized (this) {
                while (!zkElector.isElegible) {
                    zkElector.checkEligibility();
                }
            }
            System.out.println("[LEADER MANAGER]: Candidate has eligibility.");
            zkElector.elect();
            hasAuthorization = true;
            System.out.println("[LEADER MANAGER]: Succesfully elected to leader.");
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
