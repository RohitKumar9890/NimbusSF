import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();
const GIGABYTE = 1024 * 1024 * 1024;

async function main() {
    const nodes = [
        { name: 'node-1', status: 'online', capacityBytes: 10 * GIGABYTE },
        { name: 'node-2', status: 'online', capacityBytes: 10 * GIGABYTE },
        { name: 'node-3', status: 'online', capacityBytes: 10 * GIGABYTE },
    ];

    for (const node of nodes) {
        await prisma.node.upsert({
            where: { id: node.name }, // Hack to use name as a key during init if id isn't set, or just use create
            update: {},
            create: {
                id: node.name,
                name: node.name,
                status: node.status as any,
                capacityBytes: node.capacityBytes,
            },
        });
    }

    console.log('Seed data created successfully.');
}

main()
    .catch((e) => {
        console.error(e);
        process.exit(1);
    })
    .finally(async () => {
        await prisma.$disconnect();
    });
