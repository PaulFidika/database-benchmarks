// API client for SQL databases (CockroachDB and YugabyteDB)

const API_URL = 'http://localhost:9090';

export interface User {
  id: number;
  name: string;
  email: string;
  created_at: string;
}

export interface Gallery {
  id: number;
  user_id: number;
  user_name?: string;
  title: string;
  description: string;
  created_at: string;
}

export interface Comment {
  id: number;
  gallery_id: number;
  user_id: number;
  user_name: string;
  text: string;
  created_at: string;
}

type DBType = 'crdb' | 'yb';

async function fetchAPI(db: DBType, path: string, options: RequestInit = {}): Promise<Response> {
  const url = `${API_URL}/${db}${path}`;
  return fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
  });
}

// Health check
export async function checkHealth(): Promise<{ crdb: string; yb: string }> {
  try {
    const response = await fetch(`${API_URL}/health`);
    const data = await response.json();
    return { crdb: data.crdb, yb: data.yb };
  } catch {
    return { crdb: 'disconnected', yb: 'disconnected' };
  }
}

// Users
export async function createUser(db: DBType, name: string, email: string): Promise<User> {
  const response = await fetchAPI(db, '/users', {
    method: 'POST',
    body: JSON.stringify({ name, email }),
  });

  if (!response.ok) {
    throw new Error(`Failed to create user: ${response.statusText}`);
  }

  return response.json();
}

export async function getUsers(db: DBType): Promise<User[]> {
  const response = await fetchAPI(db, '/users');
  if (!response.ok) {
    throw new Error(`Failed to get users: ${response.statusText}`);
  }

  const data = await response.json();
  return data || [];
}

// Galleries
export async function createGallery(db: DBType, userId: number, title: string, description: string): Promise<Gallery> {
  const response = await fetchAPI(db, '/galleries', {
    method: 'POST',
    body: JSON.stringify({ user_id: userId, title, description }),
  });

  if (!response.ok) {
    throw new Error(`Failed to create gallery: ${response.statusText}`);
  }

  return response.json();
}

export async function getGalleries(db: DBType): Promise<Gallery[]> {
  const response = await fetchAPI(db, '/galleries');
  if (!response.ok) {
    throw new Error(`Failed to get galleries: ${response.statusText}`);
  }

  const data = await response.json();
  return data || [];
}

// Comments
export async function createComment(db: DBType, galleryId: number, userId: number, text: string): Promise<Comment> {
  const response = await fetchAPI(db, '/comments', {
    method: 'POST',
    body: JSON.stringify({ gallery_id: galleryId, user_id: userId, text }),
  });

  if (!response.ok) {
    throw new Error(`Failed to create comment: ${response.statusText}`);
  }

  return response.json();
}

export async function getComments(db: DBType, galleryId: number): Promise<Comment[]> {
  const response = await fetchAPI(db, `/comments?gallery_id=${galleryId}`);
  if (!response.ok) {
    throw new Error(`Failed to get comments: ${response.statusText}`);
  }

  const data = await response.json();
  return data || [];
}

// SSE stream for comments
export interface StreamListener {
  cancel: () => void;
}

export function subscribeToComments(
  db: DBType,
  galleryId: number,
  onComment: (comment: Comment) => void
): StreamListener {
  const eventSource = new EventSource(`${API_URL}/${db}/comments/stream?gallery_id=${galleryId}`);

  eventSource.onmessage = (event) => {
    try {
      const comment = JSON.parse(event.data) as Comment;
      onComment(comment);
    } catch (err) {
      console.error('Failed to parse comment:', err);
    }
  };

  eventSource.onerror = (err) => {
    console.error('SSE error:', err);
  };

  return {
    cancel: () => eventSource.close(),
  };
}
