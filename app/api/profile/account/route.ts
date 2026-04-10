import { NextRequest, NextResponse } from 'next/server';

import { deleteUserAccount } from '@/lib/server/data-store';
import { requireAuthenticatedUser } from '@/lib/server/session';

export const runtime = 'nodejs';

export async function DELETE(request: NextRequest) {
  const user = await requireAuthenticatedUser(request);
  if (user instanceof NextResponse) {
    return user;
  }

  await deleteUserAccount(user.id);
  return new NextResponse(null, { status: 204 });
}
