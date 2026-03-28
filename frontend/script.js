const API_BASE = 'http://localhost:8080';

// THE FRONTEND MEMORY: Track historical node statuses
let allKnownNodes = {}; 
let lastKnownFiles = [];

// 1. Polling the Cluster Status every 2 seconds
async function fetchStatus() {
    try {
        const response = await fetch(`${API_BASE}/status`);
        if (!response.ok) throw new Error("Backend offline");
        const data = await response.json();
        
        // Extract the currently active ports from the backend
        const activePorts = data.activeNodes.map(node => node.split(':')[1]);

        // Add any new nodes to our memory
        activePorts.forEach(port => {
            allKnownNodes[port] = 'alive';
        });

        // Check memory for missing nodes and mark them dead
        Object.keys(allKnownNodes).forEach(port => {
            if (!activePorts.includes(port)) {
                allKnownNodes[port] = 'dead';
            }
        });

        lastKnownFiles = data.files; // Save for optimistic rendering
        
        renderNodes();
        renderFiles();
    } catch (error) {
        document.getElementById('nodes-container').innerHTML = `<p style="color: var(--danger);">Cannot connect to Master Node. Is it running?</p>`;
    }
}

// 2. Render the Data Nodes
function renderNodes() {
    const container = document.getElementById('nodes-container');
    const ports = Object.keys(allKnownNodes).sort(); // Keep them in order 9001, 9002...

    if (ports.length === 0) {
        container.innerHTML = `<p style="color: var(--text-muted);">Waiting for heartbeats...</p>`;
        return;
    }
    
    container.innerHTML = ports.map(port => {
        const isAlive = allKnownNodes[port] === 'alive';
        const dotColor = isAlive ? 'var(--success)' : 'var(--danger)';
        const textColor = isAlive ? 'var(--text-main)' : 'var(--danger)';
        const btnStyle = isAlive ? 'btn-danger' : 'btn-disabled';
        const btnText = isAlive ? 'Kill' : 'Killed';
        const btnAction = isAlive ? `onclick="killNode('${port}')"` : 'disabled';

        return `
        <div class="node-card" style="border-left-color: ${dotColor}; opacity: ${isAlive ? '1' : '0.6'};">
            <div>
                <span class="status-dot" style="background-color: ${dotColor}"></span>
                <strong style="color: ${textColor}">Node ${port}</strong>
            </div>
            <button class="${btnStyle}" ${btnAction}>${btnText}</button>
        </div>
        `;
    }).join('');
}

// 3. Render the Files and Chunks
function renderFiles() {
    const container = document.getElementById('files-container');
    if (!lastKnownFiles || lastKnownFiles.length === 0) {
        container.innerHTML = `<p style="color: var(--text-muted);">No files stored in the cluster yet.</p>`;
        return;
    }

    container.innerHTML = lastKnownFiles.map(file => `
        <div class="file-card">
            <div class="file-header">
                <strong>📄 ${file.filename}</strong>
                <div>
                    <span style="color: var(--text-muted); margin-right: 1rem;">${(file.size / 1024).toFixed(2)} KB</span>
                    <button onclick="downloadFile('${file.filename}')">Download</button>
                </div>
            </div>
            <div class="chunk-list">
                ${file.chunks.map((chunk, index) => `
                    <div class="chunk-box">
                        <div style="color: var(--text-muted); margin-bottom: 4px;">Chunk ${index}</div>
                        ${chunk.nodes.map(node => {
                            const port = node.split(':')[1];
                            const isAlive = allKnownNodes[port] === 'alive';
                            const tagColor = isAlive ? 'var(--success)' : 'var(--danger)';
                            return `<span class="node-tag" style="background-color: ${tagColor}">${port}</span>`;
                        }).join('')}
                    </div>
                `).join('')}
            </div>
        </div>
    `).join('');
}

// 4. API Actions
async function killNode(port) {
    if(confirm(`Are you sure you want to assassinate Node ${port}?`)) {
        // OPTIMISTIC UI UPDATE: Paint it red instantly before the backend even confirms it
        allKnownNodes[port] = 'dead';
        renderNodes(); 
        renderFiles(); 

        // Send the actual kill command to the backend
        await fetch(`${API_BASE}/kill?port=${port}`);
    }
}

function downloadFile(filename) {
    window.location.href = `${API_BASE}/download?filename=${filename}`;
}

async function handleFileUpload(file) {
    if (!file) return;
    document.querySelector('.upload-zone p').innerText = "Uploading and splitting file...";
    
    try {
        await fetch(`${API_BASE}/upload?filename=${file.name}`, {
            method: 'POST',
            body: file
        });
        document.querySelector('.upload-zone p').innerText = "Drag & Drop a file here, or click to browse";
        fetchStatus(); 
    } catch (error) {
        alert("Upload failed. Is the cluster running?");
        document.querySelector('.upload-zone p').innerText = "Drag & Drop a file here, or click to browse";
    }
}

// Drag and drop visuals
const dropZone = document.getElementById('drop-zone');
dropZone.addEventListener('dragover', (e) => { e.preventDefault(); dropZone.style.borderColor = 'var(--accent)'; });
dropZone.addEventListener('dragleave', (e) => { e.preventDefault(); dropZone.style.borderColor = 'var(--border)'; });
dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.style.borderColor = 'var(--border)';
    if (e.dataTransfer.files.length > 0) handleFileUpload(e.dataTransfer.files[0]);
});

// Start polling the server immediately
fetchStatus();
setInterval(fetchStatus, 2000);