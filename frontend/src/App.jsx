import React, { useState, useRef } from 'react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
const WS_URL = import.meta.env.VITE_WS_URL || 'ws://localhost:8000';

function App() {
  const [file, setFile] = useState(null);
  const [apiKey, setApiKey] = useState('');
  const [status, setStatus] = useState(null);
  const [progress, setProgress] = useState(0);
  const [columns, setColumns] = useState([]);
  const [rows, setRows] = useState([]);
  const [filters, setFilters] = useState([]);
  const wsRef = useRef(null);

  const fetchData = async (table, flts = []) => {
    const headers = { 'X-API-Key': apiKey };
    const colRes = await axios.get(`${API_URL}/tables/${table}/columns`, {
      headers,
    });
    setColumns(colRes.data.columns);
    const queryRes = await axios.post(
      `${API_URL}/tables/${table}/query`,
      { filters: flts, limit: 20, offset: 0, fields: colRes.data.columns },
      { headers }
    );
    setRows(queryRes.data.rows);
  };

  const applyFilters = async () => {
    if (!status?.table) return;
    await fetchData(status.table, filters);
  };

  const addFilter = () => {
    setFilters([...filters, { column: '', op: 'eq', value: '' }]);
  };

  const updateFilter = (idx, field, value) => {
    const copy = [...filters];
    copy[idx][field] = value;
    setFilters(copy);
  };

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
        if (data.status === 'completed' && data.table) {
          fetchData(data.table);
        }
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

      {status?.table && (
        <div>
          <h2>Data Preview</h2>
          <button onClick={addFilter}>Add Filter</button>
          {filters.map((f, idx) => (
            <div key={idx}>
              <select
                data-testid={`column-${idx}`}
                value={f.column}
                onChange={(e) => updateFilter(idx, 'column', e.target.value)}
              >
                <option value="">Column</option>
                {columns.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
              <select
                data-testid={`op-${idx}`}
                value={f.op}
                onChange={(e) => updateFilter(idx, 'op', e.target.value)}
              >
                <option value="eq">=</option>
                <option value="neq">!=</option>
                <option value="lt">&lt;</option>
                <option value="lte">&le;</option>
                <option value="gt">&gt;</option>
                <option value="gte">&ge;</option>
                <option value="like">like</option>
                <option value="ilike">ilike</option>
                <option value="ieq">ieq</option>
              </select>
              <input
                data-testid={`value-${idx}`}
                value={f.value}
                onChange={(e) => updateFilter(idx, 'value', e.target.value)}
              />
            </div>
          ))}
          {filters.length > 0 && (
            <button onClick={applyFilters}>Apply Filters</button>
          )}
          <table>
            <thead>
              <tr>
                {columns.map((c) => (
                  <th key={c}>{c}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i}>
                  {columns.map((c) => (
                    <td key={c}>{row[c]}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default App;
