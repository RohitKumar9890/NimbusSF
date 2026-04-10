export type UserRole = 'admin' | 'user';

export interface User {
  email: string;
  name?: string;
  role: UserRole;
}

export interface FileInfo {
  id: string;
  name: string;
  size: number;
  uploadedAt: string;
  ownerId: string;
  chunks: ChunkInfo[];
  mimeType?: string;
}


export interface ChunkInfo {
  id: string;
  index: number;
  nodeId: string;
  size: number;
}

export interface NodeStatus {
  id: string;
  name: string;
  status: 'online' | 'offline';
  storage: {
    used: number;
    total: number;
  };
  chunks: number;
}

export interface UploadProgress {
  loaded: number;
  total: number;
  percentage: number;
}

export type DashboardTab = 'overview' | 'files' | 'upload' | 'nodes' | 'settings' | 'users' | 'admin-files';

export interface ManagedUser {
  id: string;
  email: string;
  name: string;
  role: UserRole;
  storageUsed: number;
  filesCount: number;
  lastActive: string;
  createdAt: string;
}

export interface AdminFile {
  id: string;
  name: string;
  size: number;
  owner: {
    id: string;
    name: string;
    email: string;
  };
  uploadedAt: string;
  chunks: number;
  nodes: string[];
}
