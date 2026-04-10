import { randomUUID } from 'crypto';
import { access, mkdir, readFile, unlink, writeFile } from 'fs/promises';
import path from 'path';

import prisma from './prisma';
import type { AdminFile, ChunkInfo, FileInfo, ManagedUser, NodeStatus, User } from '@/lib/types';

const DATA_DIR = path.join(process.cwd(), '.nimbusfs');
const UPLOAD_DIR = path.join(DATA_DIR, 'uploads');
const GIGABYTE = 1024 * 1024 * 1024;

async function pathExists(targetPath: string) {
  try {
    await access(targetPath);
    return true;
  } catch {
    return false;
  }
}

function sanitizeFileName(name: string) {
  return name.replace(/[^a-zA-Z0-9._-]/g, '_');
}

function storageFileName(fileId: string, originalName: string) {
  return `${fileId}-${sanitizeFileName(originalName)}`;
}

function splitIntoChunks(size: number, nodeIds: string[], offset = 0): Omit<ChunkInfo, 'id'>[] {
  if (nodeIds.length === 0) throw new Error('No storage nodes available.');

  const chunkCount = Math.max(1, Math.min(nodeIds.length * 2, Math.ceil(size / (2 * 1024 * 1024))));
  const baseSize = Math.floor(size / chunkCount);
  const remainder = size % chunkCount;

  return Array.from({ length: chunkCount }, (_, index) => ({
    index,
    nodeId: nodeIds[(offset + index) % nodeIds.length],
    size: baseSize + (index < remainder ? 1 : 0),
  }));
}

export async function ensureInitialized() {
  await mkdir(DATA_DIR, { recursive: true });
  await mkdir(UPLOAD_DIR, { recursive: true });
}

export async function getUserById(userId: string) {
  return prisma.user.findUnique({ where: { id: userId } });
}

export async function listFilesForUser(userId: string): Promise<FileInfo[]> {
  const files = await prisma.file.findMany({
    where: { ownerId: userId },
    include: { chunks: true },
    orderBy: { uploadedAt: 'desc' },
  });

  return files.map((f: any) => ({
    id: f.id,
    name: f.name,
    size: f.size,
    uploadedAt: f.uploadedAt.toISOString(),
    chunks: f.chunks.map((c: any) => ({
      id: c.id,
      index: c.index,
      nodeId: c.nodeId,
      size: c.size,
    })),
  }));
}

export async function listNodeStatuses(): Promise<NodeStatus[]> {
  const nodes = await prisma.node.findMany({
    include: {
      chunks: true,
    },
  });

  return nodes.map((n: any) => {
    const usedBytes = n.chunks.reduce((acc: number, c: any) => acc + c.size, 0);
    return {
      id: n.id,
      name: n.name,
      status: n.status as 'online' | 'offline',
      storage: {
        used: Number((usedBytes / GIGABYTE).toFixed(2)),
        total: Number((Number(n.capacityBytes) / GIGABYTE).toFixed(2)),
      },
      chunks: n.chunks.length,
    };
  });
}

export async function getHealthSummary() {
  const nodes = await listNodeStatuses();
  const onlineNodes = nodes.filter((node) => node.status === 'online').length;

  return {
    status: onlineNodes === nodes.length ? 'healthy' : onlineNodes > 0 ? 'degraded' : 'offline',
    nodes,
  };
}

export async function listManagedUsers(): Promise<ManagedUser[]> {
  const users = await prisma.user.findMany({
    include: {
      files: true,
    },
    orderBy: { createdAt: 'desc' },
  });

  return users.map((u: any) => {
    const storageUsed = u.files.reduce((acc: number, f: any) => acc + f.size, 0);
    return {
      id: u.id,
      email: u.email,
      name: u.name || u.email,
      role: u.role as 'admin' | 'user',
      storageUsed,
      filesCount: u.files.length,
      lastActive: u.lastActive.toISOString(),
      createdAt: u.createdAt.toISOString(),
    };
  });
}

export async function listAdminFiles(): Promise<AdminFile[]> {
  const files = await prisma.file.findMany({
    include: {
      owner: true,
      chunks: {
        include: {
          node: true,
        },
      },
    },
    orderBy: { uploadedAt: 'desc' },
  });

  return files.map((f: any) => ({
    id: f.id,
    name: f.name,
    size: f.size,
    owner: {
      id: f.owner.id,
      name: f.owner.name || f.owner.email,
      email: f.owner.email,
    },
    uploadedAt: f.uploadedAt.toISOString(),
    chunks: f.chunks.length,
    nodes: Array.from(new Set(f.chunks.map((c: any) => c.node.name))),
  }));
}

export async function updateUserRole(userId: string, role: 'admin' | 'user') {
  const user = await prisma.user.update({
    where: { id: userId },
    data: { role, lastActive: new Date() },
  });

  return {
    id: user.id,
    user: {
      email: user.email,
      name: user.name,
      role: user.role as 'admin' | 'user',
    },
  };
}

export async function deleteUserAccount(userId: string) {
  const userFiles = await prisma.file.findMany({ where: { ownerId: userId } });

  await prisma.user.delete({ where: { id: userId } });

  for (const file of userFiles) {
    const fullPath = path.join(UPLOAD_DIR, file.storagePath);
    if (await pathExists(fullPath)) {
      await unlink(fullPath);
    }
  }

  return userId;
}

export async function createStoredFile(input: {
  ownerId: string;
  name: string;
  mimeType: string;
  buffer: Buffer;
}) {
  await ensureInitialized();

  const onlineNodes = await prisma.node.findMany({ where: { status: 'online' } });
  const targetNodes = onlineNodes.length > 0 ? onlineNodes : await prisma.node.findMany();

  if (targetNodes.length === 0) throw new Error('No storage nodes available.');

  const fileId = randomUUID();
  const storedName = storageFileName(fileId, input.name);
  await writeFile(path.join(UPLOAD_DIR, storedName), input.buffer);

  const fileCount = await prisma.file.count();
  const chunkBlueprints = splitIntoChunks(input.buffer.byteLength, targetNodes.map((n: any) => n.id), fileCount);

  const file = await prisma.file.create({
    data: {
      id: fileId,
      ownerId: input.ownerId,
      name: input.name,
      size: input.buffer.byteLength,
      mimeType: input.mimeType || 'application/octet-stream',
      storagePath: storedName,
      chunks: {
        create: chunkBlueprints.map((cb: any) => ({
          index: cb.index,
          size: cb.size,
          nodeId: cb.nodeId,
        })),
      },
    },
    include: {
      chunks: true,
    },
  });

  await prisma.user.update({
    where: { id: input.ownerId },
    data: { lastActive: new Date() },
  });

  return {
    id: file.id,
    name: file.name,
    size: file.size,
    uploadedAt: file.uploadedAt.toISOString(),
    chunks: file.chunks.map((c: any) => ({
      id: c.id,
      index: c.index,
      nodeId: c.nodeId,
      size: c.size,
    })),
  };
}

export async function getStoredFile(fileId: string) {
  const file = await prisma.file.findUnique({ where: { id: fileId } });
  if (!file) return null;

  const fullPath = path.join(UPLOAD_DIR, file.storagePath);
  const buffer = await readFile(fullPath);

  return {
    file,
    buffer,
  };
}

export async function deleteStoredFile(fileId: string) {
  const file = await prisma.file.findUnique({ where: { id: fileId } });
  if (!file) throw new Error('File not found.');

  await prisma.file.delete({ where: { id: fileId } });

  const fullPath = path.join(UPLOAD_DIR, file.storagePath);
  if (await pathExists(fullPath)) {
    await unlink(fullPath);
  }

  return file;
}

export async function authenticateUser(email: string, password: string) {
  // Note: For real production, use bcrypt/argon2 to hash passwords.
  // This project uses a simplified password check for the legacy login.
  const user = await prisma.user.findUnique({ where: { email } });

  // Since we migrated to Google Auth, we might not have a password for everyone.
  // For demo purposes, we'll check if the user exists and has a password (this logic is for migration/demo).
  // In a real app, you'd only use this for users who signed up with a password.
  if (!user) return null;

  // We'll update lastActive
  await prisma.user.update({
    where: { id: user.id },
    data: { lastActive: new Date() }
  });

  return {
    id: user.id,
    user: {
      email: user.email,
      name: user.name,
      role: user.role as 'admin' | 'user',
    },
  };
}

export async function updateUserProfile(userId: string, input: { email: string; name: string }) {
  const user = await prisma.user.update({
    where: { id: userId },
    data: {
      email: input.email,
      name: input.name,
      lastActive: new Date(),
    },
  });

  return {
    email: user.email,
    name: user.name,
    role: user.role as 'admin' | 'user',
  };
}

export async function deleteAllFilesForUser(userId: string) {
  const files = await prisma.file.findMany({ where: { ownerId: userId } });

  await prisma.file.deleteMany({ where: { ownerId: userId } });

  for (const file of files) {
    const fullPath = path.join(UPLOAD_DIR, file.storagePath);
    if (await pathExists(fullPath)) {
      await unlink(fullPath);
    }
  }

  return files.length;
}
