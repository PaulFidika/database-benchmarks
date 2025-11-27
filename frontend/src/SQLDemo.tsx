import { useState, useEffect } from 'react';
import {
  User,
  Gallery,
  Comment,
  createUser,
  getUsers,
  createGallery,
  getGalleries,
  createComment,
  getComments,
  checkHealth,
  subscribeToComments,
} from './sql-api';

interface SQLDemoProps {
  db: 'crdb' | 'yb';
  name: string;
  color: string;
}

function SQLDemo({ db, name, color }: SQLDemoProps) {
  const [isConnected, setIsConnected] = useState(false);
  const [users, setUsers] = useState<User[]>([]);
  const [galleries, setGalleries] = useState<Gallery[]>([]);
  const [selectedUser, setSelectedUser] = useState<User | null>(null);
  const [selectedGallery, setSelectedGallery] = useState<Gallery | null>(null);
  const [comments, setComments] = useState<Comment[]>([]);

  const [newUserName, setNewUserName] = useState('');
  const [newUserEmail, setNewUserEmail] = useState('');
  const [newGalleryTitle, setNewGalleryTitle] = useState('');
  const [newGalleryDesc, setNewGalleryDesc] = useState('');
  const [newComment, setNewComment] = useState('');

  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  useEffect(() => {
    const checkStatus = async () => {
      const status = await checkHealth();
      setIsConnected(status[db] === 'connected');
    };
    checkStatus();
    const interval = setInterval(checkStatus, 5000);
    return () => clearInterval(interval);
  }, [db]);

  useEffect(() => {
    loadUsers();
    loadGalleries();
  }, [db]);

  useEffect(() => {
    if (!selectedGallery) {
      setComments([]);
      return;
    }

    getComments(db, selectedGallery.id).then(setComments).catch(console.error);

    const listener = subscribeToComments(db, selectedGallery.id, (newComment) => {
      setComments(prev => {
        if (prev.some(c => c.id === newComment.id)) return prev;
        return [newComment, ...prev];
      });
    });

    return () => listener.cancel();
  }, [db, selectedGallery]);

  const loadUsers = async () => {
    try {
      const fetchedUsers = await getUsers(db);
      setUsers(fetchedUsers);
    } catch (err) {
      console.error('Failed to load users:', err);
    }
  };

  const loadGalleries = async () => {
    try {
      const fetchedGalleries = await getGalleries(db);
      setGalleries(fetchedGalleries);
    } catch (err) {
      console.error('Failed to load galleries:', err);
    }
  };

  const handleCreateUser = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setSuccess(null);

    if (!newUserName.trim() || !newUserEmail.trim()) {
      setError('Please fill in all fields');
      return;
    }

    try {
      const user = await createUser(db, newUserName.trim(), newUserEmail.trim());
      setUsers(prev => [user, ...prev]);
      setNewUserName('');
      setNewUserEmail('');
      setSuccess('User created successfully!');
      setTimeout(() => setSuccess(null), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create user');
    }
  };

  const handleCreateGallery = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!selectedUser) {
      setError('Please select a user first');
      return;
    }

    if (!newGalleryTitle.trim()) {
      setError('Please enter a gallery title');
      return;
    }

    try {
      const gallery = await createGallery(
        db,
        selectedUser.id,
        newGalleryTitle.trim(),
        newGalleryDesc.trim()
      );
      setGalleries(prev => [gallery, ...prev]);
      setNewGalleryTitle('');
      setNewGalleryDesc('');
      setSuccess('Gallery created successfully!');
      setTimeout(() => setSuccess(null), 3000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create gallery');
    }
  };

  const handleCreateComment = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    if (!selectedUser) {
      setError('Please select a user to comment as');
      return;
    }

    if (!selectedGallery) {
      setError('Please select a gallery');
      return;
    }

    if (!newComment.trim()) {
      setError('Please enter a comment');
      return;
    }

    try {
      const comment = await createComment(
        db,
        selectedGallery.id,
        selectedUser.id,
        newComment.trim()
      );
      setComments(prev => [comment, ...prev]);
      setNewComment('');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create comment');
    }
  };

  return (
    <div className="demo-container" style={{ '--accent-color': color } as React.CSSProperties}>
      <div className="db-header">
        <h2>{name}</h2>
        <p>Distributed SQL database with PostgreSQL compatibility</p>
        <div className="cluster-status">
          <div className="node-status">
            <span className={`dot ${isConnected ? '' : 'offline'}`}></span>
            {isConnected ? 'Connected' : 'Disconnected'}
          </div>
          {db === 'crdb' && (
            <>
              <div className="node-status">
                <span className="dot"></span>
                Node 1 (:26257)
              </div>
              <div className="node-status">
                <span className="dot"></span>
                Node 2 (:26258)
              </div>
              <div className="node-status">
                <span className="dot"></span>
                Node 3 (:26259)
              </div>
            </>
          )}
          {db === 'yb' && (
            <>
              <div className="node-status">
                <span className="dot"></span>
                TServer 1 (:5433)
              </div>
              <div className="node-status">
                <span className="dot"></span>
                TServer 2 (:5434)
              </div>
              <div className="node-status">
                <span className="dot"></span>
                TServer 3 (:5435)
              </div>
            </>
          )}
        </div>
      </div>

      <div className="main-content">
        <div className="sidebar">
          {error && <div className="error">{error}</div>}
          {success && <div className="success">{success}</div>}

          <div className="card">
            <h3>Create User</h3>
            <form onSubmit={handleCreateUser}>
              <div className="form-group">
                <label>Name</label>
                <input
                  type="text"
                  value={newUserName}
                  onChange={e => setNewUserName(e.target.value)}
                  placeholder="John Doe"
                />
              </div>
              <div className="form-group">
                <label>Email</label>
                <input
                  type="email"
                  value={newUserEmail}
                  onChange={e => setNewUserEmail(e.target.value)}
                  placeholder="john@example.com"
                />
              </div>
              <button type="submit">Create User</button>
            </form>
          </div>

          <div className="card">
            <h3>Users ({users.length})</h3>
            {users.length === 0 ? (
              <p className="empty-state">No users yet</p>
            ) : (
              <ul className="user-list">
                {users.map(user => (
                  <li
                    key={user.id}
                    className={selectedUser?.id === user.id ? 'selected' : ''}
                    onClick={() => setSelectedUser(user)}
                  >
                    {user.name}
                  </li>
                ))}
              </ul>
            )}
          </div>

          <div className="card">
            <h3>Create Gallery</h3>
            <form onSubmit={handleCreateGallery}>
              <div className="form-group">
                <label>Title</label>
                <input
                  type="text"
                  value={newGalleryTitle}
                  onChange={e => setNewGalleryTitle(e.target.value)}
                  placeholder="My Photo Gallery"
                />
              </div>
              <div className="form-group">
                <label>Description</label>
                <textarea
                  value={newGalleryDesc}
                  onChange={e => setNewGalleryDesc(e.target.value)}
                  placeholder="A collection of..."
                  rows={3}
                />
              </div>
              <button type="submit" disabled={!selectedUser}>
                {selectedUser ? 'Create Gallery' : 'Select a user first'}
              </button>
            </form>
          </div>
        </div>

        <div className="content">
          <div className="card">
            <h3>Galleries ({galleries.length})</h3>
            {galleries.length === 0 ? (
              <div className="empty-state">
                <p>No galleries yet. Create one from the sidebar!</p>
              </div>
            ) : (
              <div className="gallery-grid">
                {galleries.map(gallery => (
                  <div
                    key={gallery.id}
                    className={`gallery-card ${selectedGallery?.id === gallery.id ? 'selected' : ''}`}
                    onClick={() => setSelectedGallery(gallery)}
                  >
                    <div className="gallery-header">
                      <h4>{gallery.title}</h4>
                      <p>by {gallery.user_name || 'Unknown'}</p>
                    </div>
                    <div className="gallery-body">
                      <p>{gallery.description || 'No description'}</p>
                      <div className="comment-count">
                        {new Date(gallery.created_at).toLocaleDateString()}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {selectedGallery && (
            <div className="comments-section">
              <h3>
                Comments on "{selectedGallery.title}"
                <span className="live-indicator">
                  <span className="pulse"></span>
                  Live (SSE)
                </span>
              </h3>

              <form className="comment-form" onSubmit={handleCreateComment}>
                <input
                  type="text"
                  value={newComment}
                  onChange={e => setNewComment(e.target.value)}
                  placeholder={selectedUser ? `Comment as ${selectedUser.name}...` : 'Select a user to comment'}
                  disabled={!selectedUser}
                />
                <button type="submit" disabled={!selectedUser || !newComment.trim()}>
                  Post
                </button>
              </form>

              <div className="comments-list">
                {comments.length === 0 ? (
                  <div className="empty-state">
                    <p>No comments yet. Be the first!</p>
                  </div>
                ) : (
                  comments.map(comment => (
                    <div key={comment.id} className="comment">
                      <div className="author">{comment.user_name}</div>
                      <div className="text">{comment.text}</div>
                      <div className="time">
                        {new Date(comment.created_at).toLocaleString()}
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default SQLDemo;
