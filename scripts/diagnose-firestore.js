const admin = require('firebase-admin');
const dotenv = require('dotenv');
const path = require('path');

dotenv.config({ path: path.join(process.cwd(), '.env.local') });

if (!admin.apps.length) {
    const privateKey = process.env.FIREBASE_PRIVATE_KEY;
    console.log('Project ID:', process.env.FIREBASE_PROJECT_ID);
    console.log('Client Email:', process.env.FIREBASE_CLIENT_EMAIL);
    console.log('Private Key Start:', privateKey?.substring(0, 30));

    try {
        admin.initializeApp({
            credential: admin.credential.cert({
                projectId: process.env.FIREBASE_PROJECT_ID,
                clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
                privateKey: privateKey ? privateKey.replace(/\\n/g, '\n') : undefined,
            }),
        });
        console.log('Firebase Admin initialized successfully.');
    } catch (err) {
        console.error('Failed to initialize Firebase Admin:', err);
        process.exit(1);
    }
}

const db = admin.firestore();

async function diagnose() {
    try {
        console.log('Testing "nodes" query...');
        const nodes = await db.collection('nodes').get();
        console.log(`Found ${nodes.size} nodes.`);

        console.log('Testing "files" query (Composite Index Check)...');
        // Simulate a query that often requires an index
        const files = await db.collection('files')
            .where('ownerId', '==', 'test-user')
            .orderBy('uploadedAt', 'desc')
            .get();
        console.log(`Found ${files.size} files for test-user.`);

    } catch (err) {
        console.error('DIAGNOSTIC FAILURE:');
        console.error(err);
        if (err.message && err.message.includes('https://console.firebase.google.com')) {
            console.log('\n--- ACTION REQUIRED ---');
            console.log('You need to create a composite index in the Firebase console.');
            console.log('Click this link to create it automatically:');
            console.log(err.message.split('at ')[1] || err.message);
        }
    }
}

diagnose();
