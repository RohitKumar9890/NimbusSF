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

async function setAdmin(email) {
    try {
        const snapshot = await db.collection('users').where('email', '==', email).limit(1).get();

        if (snapshot.empty) {
            console.log(`User ${email} not found. Creating a new admin user...`);
            await db.collection('users').doc(email).set({
                email: email,
                role: 'admin',
                name: email.split('@')[0],
                createdAt: new Date(),
                lastActive: new Date()
            });
        } else {
            const doc = snapshot.docs[0];
            await doc.ref.update({ role: 'admin' });
            console.log(`User ${email} is now an ADMIN.`);
        }
    } catch (err) {
        console.error('Error setting admin role:', err);
    }
}

setAdmin('rk8766323@gmail.com');
