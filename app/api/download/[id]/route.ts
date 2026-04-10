import { NextRequest, NextResponse } from 'next/server';

import { getStoredFile } from '@/lib/server/data-store';
import { canAccessFile, requireAuthenticatedUser } from '@/lib/server/session';

export const runtime = 'nodejs';

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ id: string }> },
) {
  const user = await requireAuthenticatedUser(request);
  if (user instanceof NextResponse) {
    return user;
  }

  const { id } = await context.params;
  const stored = await getStoredFile(id);
  if (!stored) {
    return NextResponse.json({ message: 'File not found.' }, { status: 404 });
  }

  if (!canAccessFile(user, stored.file.ownerId)) {
    return NextResponse.json({ message: 'Forbidden' }, { status: 403 });
  }

  return new NextResponse(stored.buffer, {
    headers: {
      'Content-Disposition': `attachment; filename="${encodeURIComponent(stored.file.name)}"`,
      'Content-Length': stored.file.size.toString(),
      'Content-Type': stored.file.mimeType || 'application/octet-stream',
    },
  });
}
