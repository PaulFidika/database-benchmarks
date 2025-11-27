import { useState } from 'react';
import CouchDBDemo from './CouchDBDemo';
import SQLDemo from './SQLDemo';

type Tab = 'couchdb' | 'cockroachdb' | 'yugabyte';

function App() {
  const [activeTab, setActiveTab] = useState<Tab>('couchdb');

  return (
    <div className="app">
      <header className="main-header">
        <h1>Distributed Database Demo</h1>
        <p>Compare CouchDB, CockroachDB, and YugabyteDB clusters</p>
        <div className="tabs">
          <button
            className={`tab ${activeTab === 'couchdb' ? 'active' : ''}`}
            onClick={() => setActiveTab('couchdb')}
          >
            CouchDB
          </button>
          <button
            className={`tab ${activeTab === 'cockroachdb' ? 'active' : ''}`}
            onClick={() => setActiveTab('cockroachdb')}
          >
            CockroachDB
          </button>
          <button
            className={`tab ${activeTab === 'yugabyte' ? 'active' : ''}`}
            onClick={() => setActiveTab('yugabyte')}
          >
            YugabyteDB
          </button>
        </div>
      </header>

      <div className="tab-content">
        {activeTab === 'couchdb' && <CouchDBDemo />}
        {activeTab === 'cockroachdb' && <SQLDemo db="crdb" name="CockroachDB" color="#6933ff" />}
        {activeTab === 'yugabyte' && <SQLDemo db="yb" name="YugabyteDB" color="#ff6e42" />}
      </div>
    </div>
  );
}

export default App;
