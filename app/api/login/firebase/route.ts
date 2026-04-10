import { NextRequest, NextResponse } from 'next/server';
import { adminAuth } from '@/lib/server/firebase-admin';
import { createAuthToken } from '@/lib/server/session';
import prisma from '@/lib/server/prisma';

export const runtime = 'nodejs';

export async function POST(request: NextRequest) {
    try {
        const { idToken } = await request.json();

        if (!idToken) {
            return NextResponse.json({ message: 'ID token is required' }, { status: 400 });
        }

        // Verify Firebase token
        const decodedToken = await adminAuth.verifyIdToken(idToken);
        const { email, name, picture, uid } = decodedToken;

        if (!email) {
            return NextResponse.json({ message: 'Email not found in token' }, { status: 400 });
        }

        // Upsert user in our database
        // Note: We might want to check the user role here or assign a default
        const user = await prisma.user.upsert({
            where: { email },
            update: {
                name: name || email.split('@')[0],
                lastActive: new Date(),
            },
            create: {
                email,
                name: name || email.split('@')[0],
                role: 'user', // Default role
            },
        });

        // Create session token
        const token = createAuthToken(user.id);

        return NextResponse.json({
            token,
            user: {
                email: user.email,
                name: user.name,
                role: user.role,
            },
        });
    } catch (error: any) {
        console.error('Firebase login error:', error);
        return NextResponse.json(
            { message: error.message || 'Authentication failed' },
            { status: 401 }
        );
    }
}
