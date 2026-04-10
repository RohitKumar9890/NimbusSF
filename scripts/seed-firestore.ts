import * as admin from 'firebase-admin';
import * as dotenv from 'dotenv';
import * as path from 'path';

dotenv.config({ path: path.join(process.cwd(), '.env.local') });

if (!admin.apps.length) {
    const privateKey = process.env.FIREBASE_PRIVATE_KEY;

    admin.initializeApp({
        credential: admin.credential.cert({
            projectId: process.env.FIREBASE_PROJECT_ID,
            clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
            privateKey: privateKey ? privateKey.replace(/\\n/g, '\n') : undefined,
        }),
    });
}

const db = admin.firestore();
const GIGABYTE = 1024 * 1024 * 1024;

async function main() {
    const nodes = [
        { name: 'node-1', status: 'online', capacityBytes: 10 * GIGABYTE },
        { name: 'node-2', status: 'online', capacityBytes: 10 * GIGABYTE },
        { name: 'node-3', status: 'online', capacityBytes: 10 * GIGABYTE },
    ];

    for (const node of nodes) {
        await db.collection('nodes').doc(node.name).set({
            name: node.name,
            status: node.status,
            capacityBytes: node.capacityBytes,
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
        }, { merge: true });
    }

    console.log('Seed data created successfully in Firestore.');
}

main()
    .catch((e) => {
        console.error(e);
        process.exit(1);
    });
