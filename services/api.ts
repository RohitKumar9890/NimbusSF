import axios, { AxiosError, AxiosProgressEvent } from 'axios';

import type { AdminFile, FileInfo, ManagedUser, NodeStatus, UploadProgress, User } from '@/lib/types';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || '/api';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

function getErrorMessage(error: unknown, fallback: string) {
  if (error instanceof AxiosError) {
    return (error.response?.data as { message?: string } | undefined)?.message || fallback;
  }

  if (error instanceof Error) {
    return error.message;
  }

  return fallback;
}

api.interceptors.request.use((config) => {
  if (typeof window !== 'undefined') {
    const token = localStorage.getItem('auth_token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
  }

  return config;
});

api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (typeof window !== 'undefined' && error instanceof AxiosError && error.response?.status === 401) {
      localStorage.removeItem('auth_token');
      localStorage.removeItem('user_data');
    }

    return Promise.reject(error);
  },
);

export interface LoginResponse {
  token: string;
  user: User;
}

export interface HealthResponse {
  status: string;
  nodes: NodeStatus[];
}

export const authApi = {
  login: async (email: string, password: string): Promise<LoginResponse> => {
    const response = await api.post<LoginResponse>('/login', { email, password });
    return response.data;
  },

  loginWithFirebase: async (idToken: string): Promise<LoginResponse> => {
    const response = await api.post<LoginResponse>('/login/firebase', { idToken });
    return response.data;
  },

  logout: () => {
    localStorage.removeItem('auth_token');
  },

  isAuthenticated: (): boolean => {
    if (typeof window === 'undefined') {
      return false;
    }

    return !!localStorage.getItem('auth_token');
  },
};

export const fileApi = {
  getFiles: async (): Promise<FileInfo[]> => {
    const response = await api.get<FileInfo[]>('/files');
    return response.data;
  },

  uploadFile: async (
    file: File,
    onProgress?: (progress: UploadProgress) => void,
  ): Promise<FileInfo> => {
    const formData = new FormData();
    formData.append('file', file);

    const response = await api.post<FileInfo>('/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
      onUploadProgress: (event: AxiosProgressEvent) => {
        if (!onProgress || !event.total) {
          return;
        }

        onProgress({
          loaded: event.loaded,
          total: event.total,
          percentage: Math.round((event.loaded * 100) / event.total),
        });
      },
    });

    return response.data;
  },

  downloadFile: async (fileId: string, fileName: string): Promise<void> => {
    const response = await api.get(`/download/${fileId}`, {
      responseType: 'blob',
    });

    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', fileName);
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  },

  deleteFile: async (fileId: string): Promise<void> => {
    await api.delete(`/files/${fileId}`);
  },

  deleteAllFiles: async (): Promise<void> => {
    await api.delete('/profile');
  },
};

export const healthApi = {
  getHealth: async (): Promise<HealthResponse> => {
    const response = await api.get<HealthResponse>('/health');
    return response.data;
  },

  getNodes: async (): Promise<NodeStatus[]> => {
    const response = await api.get<NodeStatus[]>('/nodes');
    return response.data;
  },
};

export const userApi = {
  getUsers: async (): Promise<ManagedUser[]> => {
    const response = await api.get<ManagedUser[]>('/users');
    return response.data;
  },

  updateRole: async (userId: string, role: 'admin' | 'user') => {
    const response = await api.patch<User>(`/users/${userId}`, { role });
    return response.data;
  },

  deleteUser: async (userId: string) => {
    await api.delete(`/users/${userId}`);
  },
};

export const adminApi = {
  getFiles: async (): Promise<AdminFile[]> => {
    const response = await api.get<AdminFile[]>('/admin/files');
    return response.data;
  },

  deleteFile: async (fileId: string) => {
    await api.delete(`/admin/files/${fileId}`);
  },
};

export const profileApi = {
  getProfile: async (): Promise<{ apiUrl: string; user: User }> => {
    const response = await api.get<{ apiUrl: string; user: User }>('/profile');
    return response.data;
  },

  updateProfile: async (input: { email: string; name: string }): Promise<User> => {
    const response = await api.patch<{ user: User }>('/profile', input);
    return response.data.user;
  },

  deleteAccount: async (): Promise<void> => {
    await api.delete('/profile/account');
  },
};

export { getErrorMessage };
export default api;
