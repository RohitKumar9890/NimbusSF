import { randomUUID } from 'crypto';
import { access, mkdir, readFile, unlink, writeFile } from 'fs/promises';
import path from 'path';

import { adminDb } from './firebase-admin';
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
  const userDoc = await adminDb.collection('users').doc(userId).get();
  if (!userDoc.exists) return null;
  return { id: userDoc.id, ...userDoc.data() } as any;
}

export async function listFilesForUser(userId: string): Promise<FileInfo[]> {
  const filesSnapshot = await adminDb.collection('files')
    .where('ownerId', '==', userId)
    .orderBy('uploadedAt', 'desc')
    .get();

  return filesSnapshot.docs.map((doc) => {
    const data = doc.data();
    return {
      id: doc.id,
      name: data.name,
      size: data.size,
      ownerId: data.ownerId,
      uploadedAt: data.uploadedAt instanceof Date ? data.uploadedAt.toISOString() : (data.uploadedAt?.toDate?.()?.toISOString() || new Date().toISOString()),
      chunks: data.chunks || [],
    };

  });
}

export async function listNodeStatuses(): Promise<NodeStatus[]> {
  const nodesSnapshot = await adminDb.collection('nodes').get();
  const nodes = nodesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as any));

  // For nodes, we need to calculate used storage from chunks
  // In Firestore, we might want to store usedBytes on node for performance, but for now we query
  const chunksSnapshot = await adminDb.collection('chunks').get();
  const allChunks = chunksSnapshot.docs.map(doc => doc.data());

  return nodes.map((n: any) => {
    const usedBytes = allChunks
      .filter((c: any) => c.nodeId === n.id)
      .reduce((acc: number, c: any) => acc + c.size, 0);

    return {
      id: n.id,
      name: n.name,
      status: n.status as 'online' | 'offline',
      storage: {
        used: Number((usedBytes / GIGABYTE).toFixed(2)),
        total: Number((n.capacityBytes / GIGABYTE).toFixed(2)),
      },
      chunks: allChunks.filter((c: any) => c.nodeId === n.id).length,
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
  const usersSnapshot = await adminDb.collection('users').orderBy('createdAt', 'desc').get();
  const filesSnapshot = await adminDb.collection('files').get();
  const allFiles = filesSnapshot.docs.map(doc => doc.data());

  return usersSnapshot.docs.map((doc: any) => {
    const u = doc.data();
    const userFiles = allFiles.filter((f: any) => f.ownerId === doc.id);
    const storageUsed = userFiles.reduce((acc: number, f: any) => acc + f.size, 0);

    return {
      id: doc.id,
      email: u.email,
      name: u.name || u.email,
      role: u.role as 'admin' | 'user',
      storageUsed,
      filesCount: userFiles.length,
      lastActive: u.lastActive instanceof Date ? u.lastActive.toISOString() : (u.lastActive?.toDate?.()?.toISOString() || new Date().toISOString()),
      createdAt: u.createdAt instanceof Date ? u.createdAt.toISOString() : (u.createdAt?.toDate?.()?.toISOString() || new Date().toISOString()),
    };
  });
}

export async function listAdminFiles(): Promise<AdminFile[]> {
  const filesSnapshot = await adminDb.collection('files').orderBy('uploadedAt', 'desc').get();
  const usersSnapshot = await adminDb.collection('users').get();
  const users = Object.fromEntries(usersSnapshot.docs.map(doc => [doc.id, doc.data()]));

  return filesSnapshot.docs.map((doc: any) => {
    const f = doc.data();
    const owner = users[f.ownerId] || { email: 'Unknown', name: 'Unknown' };

    return {
      id: doc.id,
      name: f.name,
      size: f.size,
      owner: {
        id: f.ownerId,
        name: owner.name || owner.email,
        email: owner.email,
      },
      uploadedAt: f.uploadedAt instanceof Date ? f.uploadedAt.toISOString() : (f.uploadedAt?.toDate?.()?.toISOString() || new Date().toISOString()),
      chunks: f.chunks?.length || 0,
      nodes: Array.from(new Set(f.chunks?.map((c: any) => c.nodeId) || [])),
    };
  });
}

export async function updateUserRole(userId: string, role: 'admin' | 'user') {
  await adminDb.collection('users').doc(userId).update({
    role,
    lastActive: new Date(),
  });

  const user = await getUserById(userId);
  return {
    id: userId,
    user: {
      email: user.email,
      name: user.name,
      role: user.role as 'admin' | 'user',
    },
  };
}

export async function deleteUserAccount(userId: string) {
  const filesSnapshot = await adminDb.collection('files').where('ownerId', '==', userId).get();

  // Batch delete
  const batch = adminDb.batch();
  batch.delete(adminDb.collection('users').doc(userId));

  for (const doc of filesSnapshot.docs) {
    const file = doc.data();
    batch.delete(doc.ref);
    const fullPath = path.join(UPLOAD_DIR, file.storagePath);
    if (await pathExists(fullPath)) {
      await unlink(fullPath);
    }
  }

  await batch.commit();
  return userId;
}

export async function createStoredFile(input: {
  ownerId: string;
  name: string;
  mimeType: string;
  buffer: Buffer;
}) {
  await ensureInitialized();

  const nodesSnapshot = await adminDb.collection('nodes').where('status', '==', 'online').get();
  let targetNodes = nodesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));

  if (targetNodes.length === 0) {
    const allNodesSnapshot = await adminDb.collection('nodes').get();
    targetNodes = allNodesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() }));
  }

  if (targetNodes.length === 0) throw new Error('No storage nodes available.');

  const fileId = randomUUID();
  const storedName = storageFileName(fileId, input.name);
  await writeFile(path.join(UPLOAD_DIR, storedName), input.buffer);

  const filesCountSnapshot = await adminDb.collection('files').count().get();
  const fileCount = filesCountSnapshot.data().count;
  const chunkBlueprints = splitIntoChunks(input.buffer.byteLength, targetNodes.map(n => n.id), fileCount);

  const chunks = chunkBlueprints.map((cb: any) => ({
    id: randomUUID(),
    index: cb.index,
    size: cb.size,
    nodeId: cb.nodeId,
  }));

  await adminDb.collection('files').doc(fileId).set({
    ownerId: input.ownerId,
    name: input.name,
    size: input.buffer.byteLength,
    mimeType: input.mimeType || 'application/octet-stream',
    storagePath: storedName,
    uploadedAt: new Date(),
    chunks,
  });

  // Also store chunks in a separate collection for easier global querying
  const batch = adminDb.batch();
  for (const chunk of chunks) {
    batch.set(adminDb.collection('chunks').doc(chunk.id), {
      ...chunk,
      fileId,
    });
  }
  await batch.commit();

  await adminDb.collection('users').doc(input.ownerId).update({
    lastActive: new Date(),
  });

  return {
    id: fileId,
    name: input.name,
    size: input.buffer.byteLength,
    uploadedAt: new Date().toISOString(),
    chunks,
  };
}

export async function getStoredFile(fileId: string) {
  const doc = await adminDb.collection('files').doc(fileId).get();
  if (!doc.exists) return null;

  const file = doc.data();
  const fullPath = path.join(UPLOAD_DIR, file!.storagePath);
  const buffer = await readFile(fullPath);

  return {
    file: { id: doc.id, ...file } as FileInfo,
    buffer,
  };

}

export async function deleteStoredFile(fileId: string) {
  const doc = await adminDb.collection('files').doc(fileId).get();
  if (!doc.exists) throw new Error('File not found.');

  const file = doc.data();

  // Delete chunks first
  const chunksSnapshot = await adminDb.collection('chunks').where('fileId', '==', fileId).get();
  const batch = adminDb.batch();
  chunksSnapshot.docs.forEach(doc => batch.delete(doc.ref));
  batch.delete(doc.ref);
  await batch.commit();

  const fullPath = path.join(UPLOAD_DIR, file!.storagePath);
  if (await pathExists(fullPath)) {
    await unlink(fullPath);
  }

  return { id: doc.id, ...file };
}

export async function authenticateUser(email: string, password: string) {
  const snapshot = await adminDb.collection('users').where('email', '==', email).limit(1).get();
  if (snapshot.empty) return null;

  const doc = snapshot.docs[0];
  const user = doc.data();

  await doc.ref.update({ lastActive: new Date() });

  return {
    id: doc.id,
    user: {
      email: user.email,
      name: user.name,
      role: user.role as 'admin' | 'user',
    },
  };
}

export async function updateUserProfile(userId: string, input: { email: string; name: string }) {
  await adminDb.collection('users').doc(userId).update({
    email: input.email,
    name: input.name,
    lastActive: new Date(),
  });

  const user = await getUserById(userId);
  return {
    email: user.email,
    name: user.name,
    role: user.role as 'admin' | 'user',
  };
}

export async function deleteAllFilesForUser(userId: string) {
  const filesSnapshot = await adminDb.collection('files').where('ownerId', '==', userId).get();
  const count = filesSnapshot.size;

  const batch = adminDb.batch();
  for (const doc of filesSnapshot.docs) {
    const file = doc.data();
    batch.delete(doc.ref);
    const fullPath = path.join(UPLOAD_DIR, file.storagePath);
    if (await pathExists(fullPath)) {
      await unlink(fullPath);
    }
  }

  await batch.commit();
  return count;
}
