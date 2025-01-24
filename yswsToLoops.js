import fs from 'fs';
import Airtable from 'airtable';
import { LoopsClient } from 'loops';
import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

async function fetchAllRecords(apiKey, baseId, tableName) {
  console.log(`\n📊 Fetching records from Airtable...`);
  console.log(`Base ID: ${baseId}`);
  console.log(`Table Name: ${tableName}`);

  // Configure Airtable
  Airtable.configure({ apiKey });
  const base = Airtable.base(baseId);
  
  let records = [];

  try {
    const query = await base(tableName).select();
    const pageRecords = await query.all();
    records = records.concat(pageRecords);

    console.log(`✅ Successfully fetched ${records.length} records from Airtable`);
    return records;
  } catch (error) {
    console.error('❌ Failed to fetch records:', error);
    throw error;
  }
}

async function updateLoopsContact(loops, email, data) {
  try {
    // Truncate referral reason to 490 characters if it exists
    const truncatedReferralReason = data.firstStatedReferralReason 
      ? data.firstStatedReferralReason.slice(0, 490)
      : '';

    console.log(`\n📝 Updating contact: ${email}`);
    console.log('Data to update:');
    console.log('- Weighted Grant Contribution:', data.weightedGrantContribution);
    console.log('- 2025 Weighted Grant Contribution:', data.weighted2025GrantContribution);
    console.log('- First Stated Referral Reason:', truncatedReferralReason || '(none)');
    if (data.firstStatedReferralReason && data.firstStatedReferralReason.length > 490) {
      console.log(`⚠️ Referral reason truncated from ${data.firstStatedReferralReason.length} to 490 characters`);
    }
    console.log('- First Stated Referral Category:', data.firstReferralReasonCategory || '(none)');

    await loops.updateContact(
      email,
      {
        calculatedYswsWeightedGrantContribution: data.weightedGrantContribution,
        calculated2025YswsWeightedGrantContribution: data.weighted2025GrantContribution,
        calculatedYswsFirstStatedReferralReason: truncatedReferralReason,
        calculatedYswsFirstStatedReferralCategory: data.firstReferralReasonCategory || '',
        calculatedYswsLastUpdatedAt: new Date().toISOString()
      }
    );
    
    console.log(`✅ Successfully updated contact: ${email}`);
    return true;
  } catch (error) {
    console.error(`❌ Error updating contact ${email}:`, error);
    throw error;
  }
}

// Validate environment variables
console.log('\n🔑 Validating environment variables...');
const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID || 'app3A5kJwYqxMLOgh';
const tableName = process.env.AIRTABLE_TABLE_NAME || 'tblzWWGUYHVH7Zyqf';
const loopsApiKey = process.env.LOOPS_API_KEY;

if (!apiKey) throw new Error('AIRTABLE_API_KEY is required');
if (!baseId) throw new Error('AIRTABLE_BASE_ID is required');
if (!loopsApiKey) throw new Error('LOOPS_API_KEY is required');

console.log('✅ Environment variables validated');

// Initialize Loops client
console.log('\n🔄 Initializing Loops client...');
const loops = new LoopsClient(loopsApiKey);
console.log('✅ Loops client initialized');

// Fetch and process records
const records = await fetchAllRecords(apiKey, baseId, tableName);
const yswsProjects = records.map(r => r.fields);

console.log('\n📊 Processing records...');
const emailData = yswsProjects.reduce((acc, project) => {
  let email = project['Email'];

  // Trim whitespace from the email
  if (typeof email === 'string') {
    email = email.trim();
  }

  if (!email) {
    console.warn('⚠️ Found record without email, skipping...');
    return acc;
  }

  // Enhanced regex to validate email format more strictly
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  if (!emailRegex.test(email)) {
    console.warn(`⚠️ Invalid email format: ${email}, skipping...`);
    return acc;
  }

  // Properly round to the nearest 0.1 to avoid floating-point issues
  const contribution = parseFloat((project['YSWS–Weighted Grant Contribution'] || 0).toFixed(1));

  // Check if the project is from 2025
  const projectDate = project['Approved At'] ? new Date(project['Approved At']) : new Date(project['Created'])
  const is2025Project = projectDate && projectDate.getFullYear() === 2025;

  const referralReason = project['How did you hear about this?'];
  const referralCategory = project['Referral Reason'];

  if (!acc[email]) {
    acc[email] = {
      weightedGrantContribution: 0,
      weighted2025GrantContribution: 0,
      firstStatedReferralReason: null,
      firstReferralReasonCategory: null
    };
  }
  
  acc[email].weightedGrantContribution += contribution;
  if (is2025Project) {
    acc[email].weighted2025GrantContribution += contribution;
  }

  // Only set referral info if we don't have it yet and referralReason exists
  if (!acc[email].firstStatedReferralReason && referralReason) {
    acc[email].firstStatedReferralReason = referralReason;
    acc[email].firstReferralReasonCategory = referralCategory;
  }

  // Round the total contributions to the nearest 0.1 after accumulation
  acc[email].weightedGrantContribution = parseFloat(acc[email].weightedGrantContribution.toFixed(1));
  acc[email].weighted2025GrantContribution = parseFloat(acc[email].weighted2025GrantContribution.toFixed(1));

  return acc;
}, {});

// Log processing results
console.log(`\n📊 Processing complete:`);
console.log(`- Total unique emails: ${Object.keys(emailData).length}`);
console.log(`- Total records processed: ${yswsProjects.length}`);

// Update Loops.so contacts
console.log(`\n🔄 Starting Loops.so contact updates...`);

let successCount = 0;
let failureCount = 0;

for (const [email, data] of Object.entries(emailData)) {
  try {
    await updateLoopsContact(loops, email, data);
    successCount++;
  } catch (error) {
    failureCount++;
    // Error already logged in updateLoopsContact
  }
}

// Final summary
console.log('\n📊 Final Summary:');
console.log(`✅ Successfully updated: ${successCount} contacts`);
console.log(`❌ Failed updates: ${failureCount} contacts`);
console.log(`🏁 Process completed at: ${new Date().toISOString()}`);
