import { NextRequest, NextResponse } from 'next/server';

import { authenticateUser } from '@/lib/server/data-store';
import { createAuthToken } from '@/lib/server/session';

export const runtime = 'nodejs';

export async function POST(request: NextRequest) {
  const body = (await request.json()) as { email?: string; password?: string };
  const email = body.email?.trim();
  const password = body.password?.trim();

  if (!email || !password) {
    return NextResponse.json({ message: 'Email and password are required.' }, { status: 400 });
  }

  const authResult = await authenticateUser(email, password);
  if (!authResult) {
    return NextResponse.json({ message: 'Invalid email or password.' }, { status: 401 });
  }

  return NextResponse.json({
    token: createAuthToken(authResult.id),
    user: authResult.user,
  });
}
