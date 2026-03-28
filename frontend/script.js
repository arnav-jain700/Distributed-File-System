const API_BASE = 'http://localhost:8080';

// 1. Polling the Cluster Status every 2 seconds
async function fetchStatus() {
    try {
        const response = await fetch(`${API_BASE}/status`);
        if (!response.ok) throw new Error("Backend offline");
        const data = await response.json();
        renderNodes(data.activeNodes);
        renderFiles(data.files);
    } catch (error) {
        document.getElementById('nodes-container').innerHTML = `<p style="color: var(--danger);">Cannot connect to Master Node. Is it running?</p>`;
    }
}

// 2. Render the Data Nodes
function renderNodes(nodes) {
    const container = document.getElementById('nodes-container');
    if (!nodes || nodes.length === 0) {
        container.innerHTML = `<p style="color: var(--danger);">CRITICAL: All nodes are offline!</p>`;
        return;
    }
    
    // Sort ports so they appear in order (9001, 9002, etc)
    nodes.sort();
    
    container.innerHTML = nodes.map(node => {
        const port = node.split(':')[1];
        return `
        <div class="node-card">
            <div>
                <span class="status-dot"></span>
                <strong>Node ${port}</strong>
            </div>
            <button class="btn-danger" onclick="killNode('${port}')">Kill</button>
        </div>
        `;
    }).join('');
}

// 3. Render the Files and Chunks
function renderFiles(files) {
    const container = document.getElementById('files-container');
    if (!files || files.length === 0) {
        container.innerHTML = `<p style="color: var(--text-muted);">No files stored in the cluster yet.</p>`;
        return;
    }
    container.innerHTML = files.map(file => `
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
                        ${chunk.nodes.map(node => `<span class="node-tag">${node.split(':')[1]}</span>`).join('')}
                    </div>
                `).join('')}
            </div>
        </div>
    `).join('');
}

// 4. API Actions
async function killNode(port) {
    if(confirm(`Are you sure you want to assassinate Node ${port}?`)) {
        await fetch(`${API_BASE}/kill?port=${port}`);
        fetchStatus(); // Refresh UI instantly
    }
}

function downloadFile(filename) {
    // Trigger browser native download
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
        fetchStatus(); // Refresh UI to show the new file
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
