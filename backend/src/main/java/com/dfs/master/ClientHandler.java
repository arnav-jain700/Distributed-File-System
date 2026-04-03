package com.dfs.master;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

public class ClientHandler implements Runnable {

    private final Socket clientSocket;
    private final HealthMonitor healthMonitor;

    public ClientHandler(Socket clientSocket, HealthMonitor healthMonitor) {
        this.clientSocket = clientSocket;
        this.healthMonitor = healthMonitor;
    }

    @Override
    public void run() {
        try {
            // Put a 15-second maximum wait time on the socket
            clientSocket.setSoTimeout(15000); 
            
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
                String message;
                while ((message = in.readLine()) != null) {
                    if (message.startsWith("HEARTBEAT")) {
                        String[] parts = message.split(" ");
                        if (parts.length == 2) {
                            String dataNodePort = parts[1]; 
                            String nodeAddress = "localhost:" + dataNodePort;
                            
                            System.out.println("Heartbeat received from: " + nodeAddress);
                            healthMonitor.updateHeartbeat(nodeAddress);
                        }
                    }
                }
            }
        } catch (java.net.SocketTimeoutException e) {
            // This happens if 15 seconds pass with no heartbeat!
            System.err.println("Data Node timed out (Ghosted the connection). Closing thread.");
        } catch (Exception e) {
            System.err.println("Data Node disconnected: " + e.getMessage());
        } finally {
            try { clientSocket.close(); } catch (Exception e) {}
        }
    }
}