import { NextRequest, NextResponse } from 'next/server';

import { listFilesForUser } from '@/lib/server/data-store';
import { requireAuthenticatedUser } from '@/lib/server/session';

export const runtime = 'nodejs';

export async function GET(request: NextRequest) {
  const user = await requireAuthenticatedUser(request);
  if (user instanceof NextResponse) {
    return user;
  }

  return NextResponse.json(await listFilesForUser(user.id));
}
