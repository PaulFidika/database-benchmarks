// CouchDB direct access utilities
// Browser communicates directly with CouchDB via HTTP

const COUCHDB_URL = 'http://localhost:5984';
const AUTH = btoa('admin:password');

interface CouchDBDoc {
  _id: string;
  _rev?: string;
  type: string;
  [key: string]: unknown;
}

export interface User {
  _id: string;
  _rev?: string;
  type: 'user';
  name: string;
  email: string;
  created_at: string;
}

export interface Gallery {
  _id: string;
  _rev?: string;
  type: 'gallery';
  user_id: string;
  title: string;
  description: string;
  created_at: string;
}

export interface Comment {
  _id: string;
  _rev?: string;
  type: 'comment';
  gallery_id: string;
  user_id: string;
  user_name: string;
  text: string;
  created_at: string;
}

async function fetchCouchDB(path: string, options: RequestInit = {}): Promise<Response> {
  const url = `${COUCHDB_URL}${path}`;
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Basic ${AUTH}`,
      ...options.headers,
    },
  });
  return response;
}

// Check cluster node status
export async function checkNodeStatus(port: number): Promise<boolean> {
  try {
    const response = await fetch(`http://localhost:${port}/`, {
      headers: { 'Authorization': `Basic ${AUTH}` },
    });
    return response.ok;
  } catch {
    return false;
  }
}

export async function getClusterStatus(): Promise<{ node1: boolean; node2: boolean; node3: boolean }> {
  const [node1, node2, node3] = await Promise.all([
    checkNodeStatus(5984),
    checkNodeStatus(5985),
    checkNodeStatus(5986),
  ]);
  return { node1, node2, node3 };
}

// Users
export async function createUser(name: string, email: string): Promise<User> {
  const user: Omit<User, '_rev'> = {
    _id: `user:${Date.now()}:${Math.random().toString(36).substr(2, 9)}`,
    type: 'user',
    name,
    email,
    created_at: new Date().toISOString(),
  };

  const response = await fetchCouchDB('/app_users', {
    method: 'POST',
    body: JSON.stringify(user),
  });

  if (!response.ok) {
    throw new Error(`Failed to create user: ${response.statusText}`);
  }

  const result = await response.json();
  return { ...user, _rev: result.rev };
}

export async function getUsers(): Promise<User[]> {
  const response = await fetchCouchDB('/app_users/_all_docs?include_docs=true');
  if (!response.ok) {
    throw new Error(`Failed to get users: ${response.statusText}`);
  }

  const result = await response.json();
  return result.rows
    .map((row: { doc: CouchDBDoc }) => row.doc)
    .filter((doc: CouchDBDoc) => doc.type === 'user') as User[];
}

// Galleries
export async function createGallery(userId: string, title: string, description: string): Promise<Gallery> {
  const gallery: Omit<Gallery, '_rev'> = {
    _id: `gallery:${Date.now()}:${Math.random().toString(36).substr(2, 9)}`,
    type: 'gallery',
    user_id: userId,
    title,
    description,
    created_at: new Date().toISOString(),
  };

  const response = await fetchCouchDB('/galleries', {
    method: 'POST',
    body: JSON.stringify(gallery),
  });

  if (!response.ok) {
    throw new Error(`Failed to create gallery: ${response.statusText}`);
  }

  const result = await response.json();
  return { ...gallery, _rev: result.rev };
}

export async function getGalleries(): Promise<Gallery[]> {
  const response = await fetchCouchDB('/galleries/_all_docs?include_docs=true');
  if (!response.ok) {
    throw new Error(`Failed to get galleries: ${response.statusText}`);
  }

  const result = await response.json();
  return result.rows
    .map((row: { doc: CouchDBDoc }) => row.doc)
    .filter((doc: CouchDBDoc) => doc.type === 'gallery') as Gallery[];
}

// Comments
export async function createComment(
  galleryId: string,
  userId: string,
  userName: string,
  text: string
): Promise<Comment> {
  const comment: Omit<Comment, '_rev'> = {
    _id: `comment:${Date.now()}:${Math.random().toString(36).substr(2, 9)}`,
    type: 'comment',
    gallery_id: galleryId,
    user_id: userId,
    user_name: userName,
    text,
    created_at: new Date().toISOString(),
  };

  const response = await fetchCouchDB('/comments', {
    method: 'POST',
    body: JSON.stringify(comment),
  });

  if (!response.ok) {
    throw new Error(`Failed to create comment: ${response.statusText}`);
  }

  const result = await response.json();
  return { ...comment, _rev: result.rev };
}

export async function getComments(galleryId: string): Promise<Comment[]> {
  const response = await fetchCouchDB('/comments/_all_docs?include_docs=true');
  if (!response.ok) {
    throw new Error(`Failed to get comments: ${response.statusText}`);
  }

  const result = await response.json();
  return result.rows
    .map((row: { doc: CouchDBDoc }) => row.doc)
    .filter((doc: CouchDBDoc) => doc.type === 'comment' && doc.gallery_id === galleryId) as Comment[];
}

// Real-time changes feed
export interface ChangesListener {
  cancel: () => void;
}

export function subscribeToComments(
  galleryId: string,
  onComment: (comment: Comment) => void
): ChangesListener {
  const controller = new AbortController();

  const startListening = async () => {
    try {
      // Get the current sequence number
      const infoResponse = await fetchCouchDB('/comments');
      const info = await infoResponse.json();
      let since = info.update_seq;

      while (!controller.signal.aborted) {
        try {
          const response = await fetch(
            `${COUCHDB_URL}/comments/_changes?feed=longpoll&since=${since}&include_docs=true&timeout=30000`,
            {
              headers: { 'Authorization': `Basic ${AUTH}` },
              signal: controller.signal,
            }
          );

          if (!response.ok) {
            throw new Error(`Changes feed error: ${response.statusText}`);
          }

          const changes = await response.json();
          since = changes.last_seq;

          for (const change of changes.results) {
            if (change.doc &&
                change.doc.type === 'comment' &&
                change.doc.gallery_id === galleryId &&
                !change.deleted) {
              onComment(change.doc as Comment);
            }
          }
        } catch (err) {
          if (controller.signal.aborted) break;
          // Wait a bit before retrying on error
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }
    } catch (err) {
      console.error('Changes feed error:', err);
    }
  };

  startListening();

  return {
    cancel: () => controller.abort(),
  };
}

// Subscribe to all comments (for live feed display)
export function subscribeToAllComments(
  onComment: (comment: Comment) => void
): ChangesListener {
  const controller = new AbortController();

  const startListening = async () => {
    try {
      const infoResponse = await fetchCouchDB('/comments');
      const info = await infoResponse.json();
      let since = info.update_seq;

      while (!controller.signal.aborted) {
        try {
          const response = await fetch(
            `${COUCHDB_URL}/comments/_changes?feed=longpoll&since=${since}&include_docs=true&timeout=30000`,
            {
              headers: { 'Authorization': `Basic ${AUTH}` },
              signal: controller.signal,
            }
          );

          if (!response.ok) {
            throw new Error(`Changes feed error: ${response.statusText}`);
          }

          const changes = await response.json();
          since = changes.last_seq;

          for (const change of changes.results) {
            if (change.doc && change.doc.type === 'comment' && !change.deleted) {
              onComment(change.doc as Comment);
            }
          }
        } catch (err) {
          if (controller.signal.aborted) break;
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }
    } catch (err) {
      console.error('Changes feed error:', err);
    }
  };

  startListening();

  return {
    cancel: () => controller.abort(),
  };
}
