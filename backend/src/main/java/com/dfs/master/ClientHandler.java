package com.dfs.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientHandler implements Runnable {
    
    private final Socket clientSocket;
    private final NamespaceMap namespaceMap;
    private final HealthMonitor healthMonitor;

    public ClientHandler(Socket socket, NamespaceMap namespaceMap, HealthMonitor healthMonitor) {
        this.clientSocket = socket;
        this.namespaceMap = namespaceMap;
        this.healthMonitor = healthMonitor;
    }

    @Override
    public void run() {
        // The try-with-resources block automatically closes the streams and socket when done
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
        ) {
            // Read the raw text sent over the TCP connection
            String request = in.readLine();
            
            if (request != null) {
                System.out.println("Received network request: " + request);

                // A very simple custom text protocol
                if (request.startsWith("HEARTBEAT")) {
                    // Example request: "HEARTBEAT node_8001"
                    String[] parts = request.split(" ");
                    if (parts.length == 2) {
                        String nodeId = parts[1];
                        healthMonitor.updateHeartbeat(nodeId);
                        out.println("ACK"); // Send Acknowledgment back
                    }
                } 
                // Future logic: Add "UPLOAD_FILE" and "DOWNLOAD_FILE" handlers here
                else {
                    out.println("ERROR: Unknown Command");
                }
            }
            
        } catch (IOException e) {
            System.err.println("Network I/O Error: " + e.getMessage());
        } finally {
            // Failsafe to guarantee the socket is released back to the Operating System
            try {
                if (!clientSocket.isClosed()) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.err.println("Failed to cleanly close socket.");
            }
        }
    }
}
