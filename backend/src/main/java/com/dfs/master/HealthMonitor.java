package com.dfs.master;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HealthMonitor implements Runnable {
    
    // Maps a Data Node's ID to the timestamp of its last heartbeat
    private final ConcurrentHashMap<String, Long> activeNodes;
    
    // If a node is silent for 15 seconds, consider it dead
    private final long TIMEOUT_THRESHOLD_MS = 15000; 

    public HealthMonitor() {
        this.activeNodes = new ConcurrentHashMap<>();
    }

    /**
     * Called every time a Data Node pings the Master.
     * Time Complexity: O(1)
     */
    public void updateHeartbeat(String nodeId) {
        activeNodes.put(nodeId, System.currentTimeMillis());
    }

    /**
     * This is the infinite loop that runs in the background 
     * checking the process states of all registered nodes.
     */
    @Override
    public void run() {
        while (true) {
            try {
                // Pause the thread for 5 seconds before checking again
                Thread.sleep(5000); 
                
                long currentTime = System.currentTimeMillis();
                
                // Sweep through the map to find expired timestamps
                for (Map.Entry<String, Long> entry : activeNodes.entrySet()) {
                    if (currentTime - entry.getValue() > TIMEOUT_THRESHOLD_MS) {
                        String deadNodeId = entry.getKey();
                        System.out.println("CRITICAL: Data Node " + deadNodeId + " is unresponsive.");
                        handleNodeFailure(deadNodeId);
                    }
                }
            } catch (InterruptedException e) {
                // If the system is shutting down, cleanly exit the loop
                Thread.currentThread().interrupt();
                System.out.println("Health Monitor shutting down...");
                break;
            }
        }
    }
    
    /**
     * Cleans up the system when a process dies.
     */
    private void handleNodeFailure(String deadNodeId) {
        activeNodes.remove(deadNodeId);
        // Future logic: Find which chunks were on this node 
        // and command other nodes to create new copies.
    }
}