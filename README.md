# 🚀 Distributed File System (DFS) with Real-Time Dashboard

![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=java&logoColor=white)
![JavaScript](https://img.shields.io/badge/JavaScript-F7DF1E?style=for-the-badge&logo=javascript&logoColor=black)
![HTML5](https://img.shields.io/badge/HTML5-E34F26?style=for-the-badge&logo=html5&logoColor=white)
![CSS3](https://img.shields.io/badge/CSS3-1572B6?style=for-the-badge&logo=css3&logoColor=white)

A custom-built, fault-tolerant Distributed File System engineered from scratch in Java. This project simulates the core mechanics of enterprise storage architectures (like HDFS or AWS S3), featuring custom TCP networking, real-time data replication, node health monitoring, and an interactive frontend dashboard.

## ✨ Key Features

* **Intelligent File Chunking:** Large files are automatically divided into smaller, manageable chunks (1MB) to optimize network bandwidth and memory footprint.
* **Fault Tolerance & Auto-Recovery:** Implements a replication factor of 3. If a physical node crashes or is disconnected, the Master Node instantly detects the failure and seamlessly reconstructs files using backup replicas.
* **Custom TCP Network Protocol:** Internal cluster communication bypasses heavy REST frameworks, utilizing raw TCP Sockets (`DataInputStream` / `DataOutputStream`) for ultra-low latency data streaming.
* **Concurrent Memory Safety:** Uses `ConcurrentHashMap` to manage the Master Node's state, allowing highly concurrent, thread-safe network requests without bottlenecking.
* **Real-Time Visual Dashboard:** A sleek, decoupled HTML/JS/CSS frontend that polls the cluster state to visualize live node health, file chunk distribution, and cluster topology.

## 🏗️ Architecture Overview

1. **Master Node (Control Plane):** The brain of the cluster. It provides an HTTP REST API to the frontend, manages the Namespace (knowing which chunks belong to which files), and monitors the health of the worker nodes.
2. **Data Nodes (Worker Plane):** Independent OS processes that act as dumb storage workers. They listen for raw TCP commands (STORE, READ, KILL) and use Java NIO to write file bytes directly to the physical disk.
3. **Frontend Dashboard:** A vanilla web application that provides a drag-and-drop interface and real-time visualization of the cluster's internal state.


## 🚀 Getting Started

Follow these steps to get the entire cluster running on your local machine.

### Prerequisites
* **Java Development Kit (JDK 11 or higher):** Ensure Java is installed and added to your system's PATH.
* **Visual Studio Code (Recommended):** With the "Extension Pack for Java" installed.
* **A Modern Web Browser:** Chrome, Edge, Safari, or Firefox.

### Step 1: Clone the Repository
```bash
git clone [https://github.com/Harkeerat9406/distributed-file-system-java.git](https://github.com/Harkeerat9406/distributed-file-system-java.git)
cd distributed-file-system-java
```

### Step 2: Boot up the Cluster
If you are using VS Code, a `.vscode/launch.json` file is already provided to make starting the cluster a 1-click process.

1. Open the project folder in VS Code.
2. Go to the **Run and Debug** sidebar (`Ctrl+Shift+D` or `Cmd+Shift+D`).
3. In the dropdown menu at the top, select **Run MasterNode** and hit the green Play button.
4. Once the Master Node says it is listening on port 8080, use the dropdown to run **DataNode (9001)** through **DataNode (9005)** one by one. 
5. You should now see 6 terminals running in VS Code, and the Master Node will log that it is receiving heartbeats.


### Step 3: Launch the Dashboard
Because the frontend is fully decoupled, you do not need a web server to run it.
1. Open your computer's native File Explorer/Finder.
2. Navigate to the `frontend` folder inside the project.
3. Double-click the `index.html` file to open it directly in your web browser.

---

## 🧪 Testing the Magic (Usage Guide)

Once the dashboard is open, try the following to test the architecture:

1. **Upload a File:** Drag and drop a PDF, image, or text file into the dashed upload zone. Watch as the dashboard maps out exactly which chunks were sent to which nodes.
2. **Test Fault Tolerance (The Chaos Monkey Test):**
   * Look at the chunk map for your uploaded file and note which node holds a piece of data.
   * Click the red **"Kill"** button next to that node on the left panel. The node will instantly self-destruct and turn gray.
   * Click **"Download"** on your file. 
   * **Result:** The file will still download flawlessly. The Master Node's logic automatically detects the dead node and routes the request to a surviving backup replica!


---

## 📂 Project Structure

```text
📦 distributed-file-system
 ┣ 📂 src
 ┃ ┣ 📂 com/dfs/master          # Control plane logic & HTTP API
 ┃ ┣ 📂 com/dfs/datanode        # Worker plane logic & Disk I/O
 ┃ ┗ 📂 com/dfs/shared/models   # Shared metadata blueprints
 ┣ 📂 frontend                  # Decoupled web dashboard
 ┃ ┣ 📜 index.html
 ┃ ┣ 📜 style.css
 ┃ ┗ 📜 script.js
 ┣ 📂 storage_sim               # Auto-generated simulated hard drives
 ┗ 📂 .vscode                   # VS Code cluster launch configurations
