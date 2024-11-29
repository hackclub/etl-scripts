import dotenv from 'dotenv';
import Airtable from 'airtable';
import { LoopsClient, RateLimitExceededError } from 'loops';

dotenv.config();

// Validate required environment variables
const requiredEnvVars = ['AIRTABLE_API_KEY', 'HIGH_SEAS_AIRTABLE_BASE_ID', 'LOOPS_API_KEY'];
for (const envVar of requiredEnvVars) {
    if (!process.env[envVar]) {
        throw new Error(`Missing required environment variable: ${envVar}`);
    }
}

// Field mappings from Airtable to Loops
const FIELD_MAPPINGS = {
    email: 'email',
    referralLink: 'referral_link',
    hoursLogged: 'waka_total_hours_logged',
    doubloonsPaid: 'doubloons_paid',
    doubloonsGranted: 'doubloons_granted',
    doubloonsSpent: 'doubloons_spent',
    doubloonsBalance: 'doubloons_balance'
};

// Initialize clients
const airtable = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY });
const base = airtable.base(process.env.HIGH_SEAS_AIRTABLE_BASE_ID);
const loops = new LoopsClient(process.env.LOOPS_API_KEY);

async function updateLoopsContact(email, data) {
    if (!email) {
        console.error('Skipping record: No email provided');
        return;
    }

    try {
        await loops.updateContact(email, data);
        console.log(`✓ Updated contact ${email} in Loops`);
    } catch (error) {
        if (error instanceof RateLimitExceededError) {
            console.warn(`⚠️  Rate limit hit for ${email}, retrying in 1s...`);
            await new Promise(resolve => setTimeout(resolve, 1000));
            return updateLoopsContact(email, data);
        } else {
            console.error(`✗ Failed to update ${email}:`, error.message);
            throw error; // Propagate error for handling in processRecords
        }
    }
}

async function processRecords(records) {
    console.log(`Processing ${records.length} records...`);
    let successCount = 0;
    let errorCount = 0;

    for (const record of records) {
        try {
            const email = record.get(FIELD_MAPPINGS.email);
            const data = {
                highSeasReferralLink: record.get(FIELD_MAPPINGS.referralLink),
                highSeasHoursLogged: record.get(FIELD_MAPPINGS.hoursLogged),
                highSeasDoubloonsEarned: (record.get(FIELD_MAPPINGS.doubloonsPaid) || 0) + 
                                       (record.get(FIELD_MAPPINGS.doubloonsGranted) || 0),
                highSeasDoubloonsSpent: record.get(FIELD_MAPPINGS.doubloonsSpent),
                highSeasDoubloonsBalance: record.get(FIELD_MAPPINGS.doubloonsBalance),
                highSeasLastSyncedFromAirtable: new Date().toISOString()
            };

            await updateLoopsContact(email, data);
            successCount++;
        } catch (error) {
            errorCount++;
            // Continue processing other records
            continue;
        }
    }

    console.log(`Batch complete: ${successCount} succeeded, ${errorCount} failed`);
    return { successCount, errorCount };
}

// Main execution
console.log('Starting High Seas Airtable to Loops sync...');
let totalSuccess = 0;
let totalError = 0;

try {
    let query = base('people').select({
        pageSize: 100 // Process 100 records at a time
    });
    let pageCount = 0;
    
    let records = await query.eachPage(async function page(records, fetchNextPage) {
        pageCount++;
        console.log(`\nProcessing page ${pageCount}...`);
        
        const { successCount, errorCount } = await processRecords(records);
        totalSuccess += successCount;
        totalError += errorCount;

        await fetchNextPage();
    });
    
    console.log('\nSync complete! 🎉');
    console.log(`Total records processed: ${totalSuccess + totalError}`);
    console.log(`Success: ${totalSuccess}`);
    console.log(`Failed: ${totalError}`);
} catch (error) {
    console.error('\n❌ Sync failed:', error.message);
    process.exit(1);
}
