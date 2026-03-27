package com.dfs.datanode;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.UUID;

public class DataNode {
    
    // The unique identifier for this specific worker process
    private final String nodeId;
    private final String masterHost = "127.0.0.1"; // Localhost
    private final int masterPort = 8080;
    
    // We keep the socket open for continuous communication
    private Socket socket;
    private PrintWriter out;

    public DataNode(String portOrId) {
        // In a real system, nodes are often identified by their IP/Port. 
        // Here, we assign a unique name based on what we pass via the command line.
        this.nodeId = "node_" + portOrId + "_" + UUID.randomUUID().toString().substring(0, 5);
    }

    public void start() {
        System.out.println("Starting Data Node: " + nodeId);
        
        try {
            // 1. Establish a TCP connection to the Master Node
            socket = new Socket(masterHost, masterPort);
            out = new PrintWriter(socket.getOutputStream(), true);
            System.out.println("Successfully connected to Master Node at " + masterHost + ":" + masterPort);

            // 2. Start the Heartbeat mechanism in a background thread
            startHeartbeat();

            // 3. Keep the main process alive (Future: listen for incoming chunk data here)
            // For now, we just loop infinitely to keep the process running
            while (!socket.isClosed()) {
                Thread.sleep(1000); 
            }

        } catch (IOException | InterruptedException e) {
            System.err.println("Data Node disconnected: " + e.getMessage());
        } finally {
            shutdown();
        }
    }

    /**
     * Spins up a background thread that pings the Master Node every 5 seconds.
     */
    private void startHeartbeat() {
        Thread heartbeatThread = new Thread(() -> {
            while (!socket.isClosed()) {
                try {
                    // Send the custom protocol message we defined in the Master Node
                    out.println("HEARTBEAT " + nodeId);
                    System.out.println("Sent heartbeat to Master...");
                    
                    // Wait 5 seconds before pinging again
                    Thread.sleep(5000); 
                } catch (InterruptedException e) {
                    System.err.println("Heartbeat interrupted.");
                    break;
                }
            }
        });
        
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    /**
     * Clean up OS resources before the process terminates.
     */
    private void shutdown() {
        System.out.println("Initiating clean shutdown for " + nodeId);
        try {
            if (out != null) out.close();
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException e) {
            System.err.println("Error closing sockets during shutdown.");
        }
    }

    public static void main(String[] args) {
        // We require exactly one argument from the command line: the node's port/identifier
        if (args.length < 1) {
            System.err.println("Usage: java DataNode <node_identifier>");
            System.exit(1);
        }

        DataNode node = new DataNode(args[0]);
        // Add a shutdown hook to catch CTRL+C or SIGTERM from the OS
        Runtime.getRuntime().addShutdownHook(new Thread(node::shutdown));
        
        node.start();
    }
}