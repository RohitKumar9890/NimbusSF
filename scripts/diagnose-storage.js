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
        storageBucket: process.env.NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET,
    });
}

const storage = admin.storage();
const bucket = storage.bucket();

async function testStorage() {
    console.log('Testing Storage Bucket:', process.env.NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET);
    try {
        const [exists] = await bucket.exists();
        console.log('Bucket exists:', exists);

        if (!exists) {
            console.error('ERROR: The storage bucket does not exist. Please enable it in the Firebase Console.');
            return;
        }

        console.log('Attempting a test write...');
        const file = bucket.file('test-file.txt');
        await file.save('Hello Firebase!', {
            contentType: 'text/plain',
        });
        console.log('Test write successful.');

        console.log('Attempting to delete test file...');
        await file.delete();
        console.log('Test delete successful.');

        console.log('\n--- SUCCESS ---');
        console.log('Firebase Storage is fully functional.');

    } catch (err) {
        console.error('STORAGE DIAGNOSTIC FAILURE:');
        console.error(err);
    }
}

testStorage();
