import { NextRequest, NextResponse } from 'next/server';

import { createStoredFile } from '@/lib/server/data-store';
import { requireAuthenticatedUser } from '@/lib/server/session';

export const runtime = 'nodejs';

export async function POST(request: NextRequest) {
  const user = await requireAuthenticatedUser(request);
  if (user instanceof NextResponse) {
    return user;
  }

  const formData = await request.formData();
  const uploadedFile = formData.get('file');
  if (!(uploadedFile instanceof File)) {
    return NextResponse.json({ message: 'A file upload is required.' }, { status: 400 });
  }

  const buffer = Buffer.from(await uploadedFile.arrayBuffer());
  const fileInfo = await createStoredFile({
    ownerId: user.id,
    name: uploadedFile.name,
    mimeType: uploadedFile.type,
    buffer,
  });

  return NextResponse.json(fileInfo, { status: 201 });
}
