import { SyncEngine } from '../shared/core.js';
import dotenv from 'dotenv';

// Load .env
dotenv.config();

/**
 * Main Entry Point for Node.js
 */
async function main() {
    console.log("=== Kindle Sync (Node.js) ===");
    
    // 1. Get Config
    const RSS_URL = process.env.GOODREADS_RSS_URL;
    const HC_TOKEN = process.env.HARDCOVER_API_TOKEN;
    const DRY_RUN = process.env.DRY_RUN === 'true';

    if (!RSS_URL || !HC_TOKEN) {
        console.error("❌ Stats: Missing Configuration. Please set GOODREADS_RSS_URL and HARDCOVER_API_TOKEN.");
        process.exit(1);
    }

    // Parse Limit
    const limitArgIndex = process.argv.indexOf('--limit');
    const LIMIT = limitArgIndex > -1 ? parseInt(process.argv[limitArgIndex + 1]) : 20;

    // 2. Initialize Engine
    const engine = new SyncEngine({
        hcToken: HC_TOKEN,
        rssUrl: RSS_URL,
        isDryRun: DRY_RUN,
        limit: LIMIT,
        onLog: (msg, type) => {
            // We can colorize output here if we want terminal colors
            // For now, pure log is fine
            // console.log already handles it in the engine for debug, but we can customize
        }
    });

    // 3. Run
    try {
        const results = await engine.run();
        console.log("\n=== Sync Summary ===");
        console.log(`New Books Added: ${results.newBooks}`);
        if(results.added.length > 0) {
            results.added.forEach(b => console.log(` - ${b.title} (ID: ${b.id})`));
        }
        if(results.errors.length > 0) {
             console.log("\nErrors encountered:");
             results.errors.forEach(e => console.log(` - ${e}`));
             process.exit(1);
        }
        console.log("Done.");
    } catch (e) {
        console.error("Critical Error:", e);
        process.exit(1);
    }
}

main();
