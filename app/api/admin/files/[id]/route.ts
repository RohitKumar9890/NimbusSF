import { NextRequest, NextResponse } from 'next/server';

import { deleteStoredFile, getStoredFile } from '@/lib/server/data-store';
import { requireAdminUser } from '@/lib/server/session';

export const runtime = 'nodejs';

export async function DELETE(
  request: NextRequest,
  context: { params: Promise<{ id: string }> },
) {
  const admin = await requireAdminUser(request);
  if (admin instanceof NextResponse) {
    return admin;
  }

  const { id } = await context.params;
  const stored = await getStoredFile(id);
  if (!stored) {
    return NextResponse.json({ message: 'File not found.' }, { status: 404 });
  }

  await deleteStoredFile(id);
  return new NextResponse(null, { status: 204 });
}
