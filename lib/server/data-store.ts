import { randomUUID } from 'crypto';
import { adminDb } from './firebase-admin';
import type { AdminFile, ChunkInfo, FileInfo, ManagedUser, NodeStatus, User } from '@/lib/types';

const GIGABYTE = 1024 * 1024 * 1024;
const CHUNK_SIZE_LIMIT = 512 * 1024; // 512KB per Firestore document (safe under 1MB limit)

export async function ensureInitialized() {
  // No local dir needed
}

export async function getUserById(userId: string) {
  const userDoc = await adminDb.collection('users').doc(userId).get();
  if (!userDoc.exists) return null;
  return { id: userDoc.id, ...userDoc.data() } as any;
}

export async function listFilesForUser(userId: string): Promise<FileInfo[]> {
  // REMOVED orderBy to avoid composite index requirement
  const filesSnapshot = await adminDb.collection('files')
    .where('ownerId', '==', userId)
    .get();

  const files = filesSnapshot.docs.map((doc) => {
    const data = doc.data();
    return {
      id: doc.id,
      name: data.name,
      size: data.size,
      uploadedAt: data.uploadedAt instanceof Date ? data.uploadedAt.toISOString() : (data.uploadedAt?.toDate?.()?.toISOString() || new Date().toISOString()),
      chunks: data.chunks || [],
      ownerId: data.ownerId,
      mimeType: data.mimeType,
    };
  });

  // Sort manually in JS
  return files.sort((a, b) => new Date(b.uploadedAt).getTime() - new Date(a.uploadedAt).getTime());
}

export async function listNodeStatuses(): Promise<NodeStatus[]> {
  const nodesSnapshot = await adminDb.collection('nodes').get();
  const nodes = nodesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as any));

  const chunksCountSnapshot = await adminDb.collection('fileChunks').count().get();
  const totalChunks = chunksCountSnapshot.data().count;

  return nodes.map((n: any) => {
    const capacityBytes = n.capacityBytes || (10 * GIGABYTE);
    return {
      id: n.id,
      name: n.name,
      status: n.status as 'online' | 'offline',
      storage: {
        used: 0,
        total: Number((capacityBytes / GIGABYTE).toFixed(2)),
      },
      chunks: Math.floor(totalChunks / nodes.length),
    };
  });
}

export async function getHealthSummary() {
  const nodes = await listNodeStatuses();
  return {
    status: 'healthy' as const,
    nodes,
  };
}

export async function listManagedUsers(): Promise<ManagedUser[]> {
  const usersSnapshot = await adminDb.collection('users').get();
  const filesSnapshot = await adminDb.collection('files').get();
  const allFiles = filesSnapshot.docs.map(doc => ({ id: doc.id, ...doc.data() } as any));

  const users = usersSnapshot.docs.map((doc: any) => {
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

  return users.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
}

export async function listAdminFiles(): Promise<AdminFile[]> {
  const filesSnapshot = await adminDb.collection('files').get();
  const usersSnapshot = await adminDb.collection('users').get();
  const users = Object.fromEntries(usersSnapshot.docs.map(doc => [doc.id, doc.data()]));

  const files = filesSnapshot.docs.map((doc: any) => {
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
      chunks: f.chunksCount || 0,
      nodes: ['cloud-db-1'],
    };
  });

  return files.sort((a, b) => new Date(b.uploadedAt).getTime() - new Date(a.uploadedAt).getTime());
}

export async function createStoredFile(input: {
  ownerId: string;
  name: string;
  mimeType: string;
  buffer: Buffer;
}) {
  const fileId = randomUUID();
  const totalSize = input.buffer.byteLength;
  const chunkCount = Math.ceil(totalSize / CHUNK_SIZE_LIMIT);

  const batch = adminDb.batch();

  batch.set(adminDb.collection('files').doc(fileId), {
    ownerId: input.ownerId,
    name: input.name,
    size: totalSize,
    mimeType: input.mimeType || 'application/octet-stream',
    uploadedAt: new Date(),
    chunksCount: chunkCount,
  });

  for (let i = 0; i < chunkCount; i++) {
    const start = i * CHUNK_SIZE_LIMIT;
    const end = Math.min(start + CHUNK_SIZE_LIMIT, totalSize);
    const chunkBuffer = input.buffer.slice(start, end);

    const chunkId = randomUUID();
    batch.set(adminDb.collection('fileChunks').doc(chunkId), {
      fileId,
      index: i,
      data: chunkBuffer,
    });
  }

  await batch.commit();

  await adminDb.collection('users').doc(input.ownerId).update({
    lastActive: new Date(),
  });

  return {
    id: fileId,
    name: input.name,
    size: totalSize,
    uploadedAt: new Date().toISOString(),
    ownerId: input.ownerId,
  };
}

export async function getStoredFile(fileId: string) {
  const fileDoc = await adminDb.collection('files').doc(fileId).get();
  if (!fileDoc.exists) return null;

  const fileMetadata = fileDoc.data();

  // REMOVED orderBy to avoid composite index requirement
  const chunksSnapshot = await adminDb.collection('fileChunks')
    .where('fileId', '==', fileId)
    .get();

  const chunks = chunksSnapshot.docs.map(doc => doc.data());
  // Sort manually in JS
  chunks.sort((a, b) => a.index - b.index);

  const buffers = chunks.map(c => c.data);
  const totalBuffer = Buffer.concat(buffers);

  return {
    file: { id: fileDoc.id, ...fileMetadata } as any,
    buffer: totalBuffer,
  };
}

export async function deleteStoredFile(fileId: string) {
  const fileDoc = await adminDb.collection('files').doc(fileId).get();
  if (!fileDoc.exists) throw new Error('File not found.');

  const file = fileDoc.data();
  const chunksSnapshot = await adminDb.collection('fileChunks').where('fileId', '==', fileId).get();

  const batch = adminDb.batch();
  chunksSnapshot.docs.forEach(doc => batch.delete(doc.ref));
  batch.delete(fileDoc.ref);

  await batch.commit();
  return { id: fileId, ...file };
}

export async function updateUserRole(userId: string, role: 'admin' | 'user') {
  await adminDb.collection('users').doc(userId).update({
    role,
    lastActive: new Date(),
  });

  const userDoc = await adminDb.collection('users').doc(userId).get();
  const userData = userDoc.data();

  return {
    id: userId,
    user: {
      email: userData?.email,
      name: userData?.name || userData?.email,
      role: userData?.role as 'admin' | 'user',
    },
  };
}

export async function deleteUserAccount(userId: string) {
  const filesSnapshot = await adminDb.collection('files').where('ownerId', '==', userId).get();

  const batch = adminDb.batch();
  for (const doc of filesSnapshot.docs) {
    const fileId = doc.id;
    const chunksSnapshot = await adminDb.collection('fileChunks').where('fileId', '==', fileId).get();
    chunksSnapshot.docs.forEach(c => batch.delete(c.ref));
    batch.delete(doc.ref);
  }

  batch.delete(adminDb.collection('users').doc(userId));
  await batch.commit();
  return userId;
}

export async function authenticateUser(email: string, password: string) {
  const snapshot = await adminDb.collection('users').where('email', '==', email).limit(1).get();
  if (snapshot.empty) return null;
  const doc = snapshot.docs[0];
  return { id: doc.id, ...doc.data() } as any;
}

export async function updateUserProfile(userId: string, input: { email: string; name: string }) {
  await adminDb.collection('users').doc(userId).update({
    email: input.email,
    name: input.name,
    lastActive: new Date(),
  });
  return { userId, ...input };
}

export async function deleteAllFilesForUser(userId: string) {
  const filesSnapshot = await adminDb.collection('files').where('ownerId', '==', userId).get();
  const count = filesSnapshot.size;

  const batch = adminDb.batch();
  for (const doc of filesSnapshot.docs) {
    const fileId = doc.id;
    const chunksSnapshot = await adminDb.collection('fileChunks').where('fileId', '==', fileId).get();
    chunksSnapshot.docs.forEach(c => batch.delete(c.ref));
    batch.delete(doc.ref);
  }

  await batch.commit();
  return count;
}
