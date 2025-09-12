
import { useState } from 'react';
import './App.css';

function App() {
  const [file, setFile] = useState(null);
  const [response, setResponse] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
    setResponse(null);
    setError(null);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setResponse(null);
    setError(null);
    if (!file) {
      setError('Please select a file.');
      setLoading(false);
      return;
    }
    try {
      // Placeholder: Replace with your actual Lambda API endpoint
      const apiUrl = 'https://lfyfn7f5x276jk3xodx6gm5i6i0iqumh.lambda-url.ap-south-1.on.aws/';
      const formData = new FormData();
      formData.append('file', file);
      const res = await fetch(apiUrl, {
        method: 'POST',
        body: formData,
      });
      if (!res.ok) throw new Error('API error');
      const data = await res.json();
      setResponse(data);
    } catch (err) {
      setError('Failed to process file.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <nav className="navbar">
        <div className="navbar-brand">Pentagon</div>
      </nav>
      <div className="container">
        <h1 className="main-title">RTFT AI</h1>
      <form onSubmit={handleSubmit} className="upload-form">
        <input type="file" onChange={handleFileChange} />
        <button type="submit" disabled={loading}>
          {loading ? 'Processing...' : 'Upload & Rename'}
        </button>
      </form>
      {error && <div className="error">{error}</div>}
      {response && (
        <div className="result">
          <h2>Renamed File Info</h2>
          <pre>{JSON.stringify(response, null, 2)}</pre>
        </div>
      )}
      </div>
    </div>
  );
}

export default App;
