import { NextRequest, NextResponse } from 'next/server';

import { listManagedUsers } from '@/lib/server/data-store';
import { requireAdminUser } from '@/lib/server/session';

export const runtime = 'nodejs';

export async function GET(request: NextRequest) {
  const user = await requireAdminUser(request);
  if (user instanceof NextResponse) {
    return user;
  }

  return NextResponse.json(await listManagedUsers());
}
