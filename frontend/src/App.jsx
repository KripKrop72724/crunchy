import React, { useState, useRef } from 'react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8000';

function App() {
  const [file, setFile] = useState(null);
  const [apiKey, setApiKey] = useState('');
  const [status, setStatus] = useState(null);
  const [progress, setProgress] = useState(0);
  const wsRef = useRef(null);

  const upload = async () => {
    if (!file) return;
    setStatus({ status: 'uploading' });
    setProgress(0);

    const form = new FormData();
    form.append('file', file);

    try {
      const response = await axios.post(`${API_URL}/upload`, form, {
        headers: { 'X-API-Key': apiKey },
        onUploadProgress: (event) => {
          if (event.total) {
            setProgress(Math.round((event.loaded / event.total) * 100));
          }
        },
      });

      const { job_id } = response.data;
      const ws = new WebSocket(`${WS_URL}/ws/status/${job_id}`);
      wsRef.current = ws;
      ws.onmessage = (ev) => {
        const data = JSON.parse(ev.data);
        setStatus(data);
      };
      ws.onerror = () => {
        setStatus({ status: 'failed', error: 'WebSocket error' });
      };
    } catch (err) {
      setStatus({ status: 'failed', error: err.message });
    }
  };

  return (
    <div>
      <h1>Crunchy Upload</h1>
      <input
        type="text"
        placeholder="API Key"
        value={apiKey}
        onChange={(e) => setApiKey(e.target.value)}
      />
      <input
        type="file"
        data-testid="file-input"
        onChange={(e) => setFile(e.target.files[0])}
      />
      <button onClick={upload} disabled={!file}>
        Upload
      </button>
      {status && (
        <div>
          <p>Status: {status.status}</p>
          {status.uploaded !== undefined && status.total !== undefined && (
            <p>
              Uploaded: {status.uploaded} / {status.total}
            </p>
          )}
          {progress > 0 && <p>Progress: {progress}%</p>}
          {status.rows !== undefined && <p>Rows: {status.rows}</p>}
          {status.error && <p>Error: {status.error}</p>}
          {status.table && <p>Table: {status.table}</p>}
        </div>
      )}
    </div>
  );
}

export default App;
