import type { NextRequest } from 'next/server';
import { NextResponse } from 'next/server';

import { getUserById } from '@/lib/server/data-store';


type TokenPayload = {
  issuedAt: string;
  userId: string;
};

const TOKEN_PREFIX = 'nimbusfs.';

function decodeToken(token: string) {
  if (!token.startsWith(TOKEN_PREFIX)) {
    return null;
  }

  try {
    const payload = JSON.parse(Buffer.from(token.slice(TOKEN_PREFIX.length), 'base64url').toString('utf8')) as TokenPayload;
    return payload.userId ? payload : null;
  } catch {
    return null;
  }
}

export function createAuthToken(userId: string) {
  const payload: TokenPayload = {
    userId,
    issuedAt: new Date().toISOString(),
  };

  return `${TOKEN_PREFIX}${Buffer.from(JSON.stringify(payload)).toString('base64url')}`;
}

export async function getAuthenticatedUser(request: NextRequest) {
  const authorization = request.headers.get('authorization');
  if (!authorization?.startsWith('Bearer ')) {
    return null;
  }

  const payload = decodeToken(authorization.slice('Bearer '.length));
  if (!payload) {
    return null;
  }

  return getUserById(payload.userId);
}

export async function requireAuthenticatedUser(request: NextRequest) {
  const user = await getAuthenticatedUser(request);
  if (!user) {
    return NextResponse.json({ message: 'Unauthorized' }, { status: 401 });
  }

  return user;
}

export async function requireAdminUser(request: NextRequest) {
  const user = await requireAuthenticatedUser(request);
  if (user instanceof NextResponse) {
    return user;
  }

  if (user.role !== 'admin') {
    return NextResponse.json({ message: 'Forbidden' }, { status: 403 });
  }

  return user;
}

export function canAccessFile(user: { id: string, role: string }, ownerId: string) {
  return user.role === 'admin' || user.id === ownerId;
}

