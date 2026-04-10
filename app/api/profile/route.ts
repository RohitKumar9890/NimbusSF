import { NextRequest, NextResponse } from 'next/server';

import { deleteAllFilesForUser, updateUserProfile } from '@/lib/server/data-store';
import { requireAuthenticatedUser } from '@/lib/server/session';

export const runtime = 'nodejs';

export async function GET(request: NextRequest) {
  const user = await requireAuthenticatedUser(request);
  if (user instanceof NextResponse) {
    return user;
  }

  return NextResponse.json({
    user: {
      email: user.email,
      name: user.name,
      role: user.role,
    },
    apiUrl: '/api',
  });
}

export async function PATCH(request: NextRequest) {
  const user = await requireAuthenticatedUser(request);
  if (user instanceof NextResponse) {
    return user;
  }

  const body = (await request.json()) as { email?: string; name?: string };
  if (!body.email?.trim()) {
    return NextResponse.json({ message: 'Email is required.' }, { status: 400 });
  }

  try {
    const updatedUser = await updateUserProfile(user.id, {
      email: body.email,
      name: (body.name ?? user.name) || '',

    });

    return NextResponse.json({ user: updatedUser });
  } catch (error) {
    return NextResponse.json(
      { message: error instanceof Error ? error.message : 'Unable to update profile.' },
      { status: 400 },
    );
  }
}

export async function DELETE(request: NextRequest) {
  const user = await requireAuthenticatedUser(request);
  if (user instanceof NextResponse) {
    return user;
  }

  await deleteAllFilesForUser(user.id);
  return new NextResponse(null, { status: 204 });
}
