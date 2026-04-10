const admin = require('firebase-admin');
const dotenv = require('dotenv');
const path = require('path');

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

async function diagnoseChunks() {
    console.log('Testing "fileChunks" query (Composite Index Check)...');
    try {
        const chunks = await db.collection('fileChunks')
            .where('fileId', '==', 'test-id')
            .orderBy('index', 'asc')
            .get();
        console.log(`Found ${chunks.size} chunks.`);
        console.log('SUCCESS: Index is already active.');
    } catch (err) {
        if (err.message && err.message.includes('https://console.firebase.google.com')) {
            console.log('\n--- ACTION REQUIRED ---');
            console.log('Create this index:');
            console.log(err.message.split('at ')[1] || err.message);
        } else {
            console.error(err);
        }
    }
}

diagnoseChunks();
