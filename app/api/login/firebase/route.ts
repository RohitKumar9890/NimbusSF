import { NextRequest, NextResponse } from 'next/server';
import { adminAuth, adminDb } from '@/lib/server/firebase-admin';
import { createAuthToken } from '@/lib/server/session';


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

        // Upsert user in Firestore
        const userRef = adminDb.collection('users').doc(uid);
        const userDoc = await userRef.get();

        const userData = {
            email,
            name: name || email.split('@')[0],
            lastActive: new Date(),
            role: userDoc.exists ? userDoc.data()?.role || 'user' : 'user',
            createdAt: userDoc.exists ? userDoc.data()?.createdAt : new Date(),
        };

        await userRef.set(userData, { merge: true });

        // Create session token
        const token = createAuthToken(uid);

        return NextResponse.json({
            token,
            user: {
                email,
                name: userData.name,
                role: userData.role,
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
