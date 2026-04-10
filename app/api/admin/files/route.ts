import { NextRequest, NextResponse } from 'next/server';

import { listAdminFiles } from '@/lib/server/data-store';
import { requireAdminUser } from '@/lib/server/session';

export const runtime = 'nodejs';

export async function GET(request: NextRequest) {
  const admin = await requireAdminUser(request);
  if (admin instanceof NextResponse) {
    return admin;
  }

  return NextResponse.json(await listAdminFiles());
}
