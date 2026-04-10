import { NextRequest, NextResponse } from 'next/server';

import { deleteUserAccount, updateUserRole } from '@/lib/server/data-store';
import { requireAdminUser } from '@/lib/server/session';

export const runtime = 'nodejs';

export async function PATCH(
  request: NextRequest,
  context: { params: Promise<{ id: string }> },
) {
  const admin = await requireAdminUser(request);
  if (admin instanceof NextResponse) {
    return admin;
  }

  const { id } = await context.params;
  const body = (await request.json()) as { role?: 'admin' | 'user' };
  if (!body.role || !['admin', 'user'].includes(body.role)) {
    return NextResponse.json({ message: 'A valid role is required.' }, { status: 400 });
  }

  try {
    const result = await updateUserRole(id, body.role);
    return NextResponse.json(result.user);
  } catch (error) {
    return NextResponse.json(
      { message: error instanceof Error ? error.message : 'Unable to update role.' },
      { status: 404 },
    );
  }
}

export async function DELETE(
  request: NextRequest,
  context: { params: Promise<{ id: string }> },
) {
  const admin = await requireAdminUser(request);
  if (admin instanceof NextResponse) {
    return admin;
  }

  const { id } = await context.params;
  if (id === admin.id) {
    return NextResponse.json({ message: 'You cannot delete your own account here.' }, { status: 400 });
  }

  try {
    await deleteUserAccount(id);
    return new NextResponse(null, { status: 204 });
  } catch (error) {
    return NextResponse.json(
      { message: error instanceof Error ? error.message : 'Unable to delete user.' },
      { status: 404 },
    );
  }
}
