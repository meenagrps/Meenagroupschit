/**
 * ============================================================================
 * MEENA CHITFUNDS - WHATSAPP AUTOMATION BACKEND SERVER (OFFICIAL CLOUD API)
 * ============================================================================
 * Architecture:
 * 1. Express server hosted on Port 5555 with native JSON webhook parsing.
 * 2. Official WhatsApp Business Cloud API Integration (HTTPS REST).
 * 3. Graceful Multi-Environment Secret Resolver (.env fallback to Cloud Platform Secrets).
 * 4. Firebase Admin SDK initialized from env OR local firebaseApiKey.json.
 * 5. High-performance, targeted Firestore reads (1 Group Read + N Defaulter Reads).
 * 6. Strict Indian Phone Number Normalization Pipeline.
 * 7. Live Dashboard with matching Meena Chitfunds Design System.
 * 8. Global Priority Engine: Auto-Dispatch Runs First, Manual Queues Second.
 * 9. Individualized Sequential Math Engine (No Global Timeline Traps).
 * 10. Dual JIT Checkpoint Architecture (Isolated Auto & Manual State Management).
 * 11. Meta-Compliant Rate Controls (5-10s member pacing, 1m inter-group settle).
 * 12. Quiet Hours (11 PM - 6 AM IST) with Mid-Loop Cutoff and Manual Overrides.
 * 13. Dynamic Admin Phone Engine & Pre-Approved Meta Utility Templates.
 * 14. Verification Handshake (GET /webhook) & Event Listener (POST /webhook).
 * 15. Zero-Read Hourly Engine: In-Memory Cycle Caching completely eliminates redundant reads.
 */

const express = require('express');
const http = require('http');
const https = require('https');
const admin = require('firebase-admin');
const path = require('path');
const fs = require('fs');

// --- 1. ENVIRONMENT CONFIGURATION & SECRETS RESOLVER ---
// Check if local .env exists; if not, seamlessly rely on cloud environment variables (Render / Hugging Face)
const localEnvPath = path.join(__dirname, '.env');
if (fs.existsSync(localEnvPath)) {
    try {
        const dotenv = require('dotenv');
        dotenv.config({ path: localEnvPath });
        console.log('[CONFIG] Successfully parsed local .env file.');
    } catch (dotenvErr) {
        console.log('[CONFIG] Notice: dotenv package not found or skipped. Reading directly from platform runtime.');
    }
} else {
    console.log('[CONFIG] Local .env not found. Utilizing platform cloud secrets (Render / Hugging Face runtime).');
}

const META_ACCESS_TOKEN = process.env.META_ACCESS_TOKEN || process.env.WHATSAPP_TOKEN;
const META_PHONE_NUMBER_ID = process.env.META_PHONE_NUMBER_ID || process.env.WHATSAPP_PHONE_NUMBER_ID;
const WEBHOOK_VERIFY_TOKEN = process.env.WEBHOOK_VERIFY_TOKEN || 'MEENA_CHITFUNDS_SECURE_VERIFY_2026';
const META_API_VERSION = process.env.META_API_VERSION || 'v20.0';

if (!META_ACCESS_TOKEN || !META_PHONE_NUMBER_ID) {
    console.warn('[WARNING] META_ACCESS_TOKEN or META_PHONE_NUMBER_ID is missing from secrets. Cloud API outbound requests will fail until set.');
}

// --- 2. FIREBASE ADMIN INITIALIZATION (ENV OR LOCAL JSON) ---
let serviceAccount;
const secretEnv = process.env.FIREBASE_SERVICE_ACCOUNT || process.env.FIREBASE_CONFIG;
const localKeyPath = path.join(__dirname, 'firebaseApiKey.json');

try {
    if (secretEnv) {
        serviceAccount = JSON.parse(secretEnv);
        console.log('[FIREBASE] Successfully parsed service account from environment variables.');
    } else if (fs.existsSync(localKeyPath)) {
        serviceAccount = require(localKeyPath);
        console.log('[FIREBASE] Successfully loaded service account from local firebaseApiKey.json.');
    } else {
        console.error('CRITICAL: Firebase credentials not found in environment variables or firebaseApiKey.json');
        process.exit(1);
    }
} catch (err) {
    console.error('[FIREBASE] Error loading Firebase configuration:', err.message);
    process.exit(1);
}

if (!admin.apps.length) {
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount)
    });
}

const db = admin.firestore();

// DUAL STATE MANAGEMENT DOCUMENTS
const AUTO_STATE_REF = db.collection('system_state').doc('whatsapp_auto_dispatch');
const MANUAL_STATE_REF = db.collection('system_state').doc('whatsapp_manual_dispatch');
console.log('[FIREBASE] Firestore Admin SDK initialized.');

// --- 3. GLOBAL STATE & DISPATCH TIMERS ---
const app = express();
app.use(express.json()); // Essential for parsing incoming Meta Webhooks
const server = http.createServer(app);
const PORT = process.env.PORT || 5555;

let connectionStatus = META_ACCESS_TOKEN && META_PHONE_NUMBER_ID ? 'connected' : 'unconfigured';
let connectedUser = 'Meta Cloud API Gateway';
let isDispatching = false; // Central lock for the Priority Master Queue
let globalCancelFlag = false; 
let isAutoDispatchPaused = false; 
let autoDispatchInterval = null;

// ZERO-READ IN-MEMORY CYCLE CACHE
// Eliminates repetitive hourly Firestore reads when current window is already marked processed
let cachedWindowKey = null;

// Delay Helpers
const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// SMART INTERRUPTIBLE DELAY FOR SECONDS (Adjusted for Meta Rate Control)
async function interruptibleWaitSeconds(minSec, maxSec, type = 'DELAY') {
    const ms = Math.floor(Math.random() * ((maxSec * 1000) - (minSec * 1000) + 1)) + (minSec * 1000);
    const intervals = Math.floor(ms / 1000); // 1-second pulse checks
    for (let i = 0; i < intervals; i++) {
        if (globalCancelFlag || isAutoDispatchPaused) {
            console.log(`[${type}] Sleep interrupted by system flag.`);
            return true; // Indicates the sleep was interrupted
        }
        await wait(1000);
    }
    return false; // Completed naturally
}

// SMART INTERRUPTIBLE DELAY FOR MINUTES (For Group Settle)
async function interruptibleWaitMinutes(min, max, type = 'DELAY') {
    const ms = Math.floor(Math.random() * ((max * 60000) - (min * 60000) + 1)) + (min * 60000);
    const intervals = Math.floor(ms / 5000); // 5-second pulse checks
    for (let i = 0; i < intervals; i++) {
        if (globalCancelFlag || isAutoDispatchPaused) {
            console.log(`[${type}] Sleep interrupted by system flag.`);
            return true; // Indicates the sleep was interrupted
        }
        await wait(5000);
    }
    return false; // Completed naturally
}

// Quiet Hours Helper (11:00 PM to 6:00 AM IST)
function isQuietHours() {
    const istString = new Date().toLocaleString("en-US", {timeZone: "Asia/Kolkata"});
    const istHour = new Date(istString).getHours();
    return istHour >= 23 || istHour < 6;
}

// --- 4. CORE CALCULATION ENGINES ---
function calculateCurrentSystemMonth(startDateStr) {
    if (!startDateStr) return 1;
    const start = new Date(startDateStr);
    const now = new Date();
    let months = (now.getFullYear() - start.getFullYear()) * 12;
    months -= start.getMonth();
    months += now.getMonth();
    return months <= 0 ? 1 : months + 1;
}

function calculateParticipantExpectedMonth(user, groupData) {
    if (!user || !groupData) return 1;
    let start;
    if (user?.joinedAt) {
        start = typeof user.joinedAt.toDate === 'function' ? user.joinedAt.toDate() : new Date(user.joinedAt);
    } else {
        start = new Date(groupData?.startDate || new Date());
    }
    const now = new Date();
    let elapsed = (now.getFullYear() - start.getFullYear()) * 12;
    elapsed -= start.getMonth();
    elapsed += now.getMonth();
    if (elapsed < 0) elapsed = 0;

    let expected = elapsed + 1;
    const totalGroupMonths = groupData.totalMonths || 0;
    if (totalGroupMonths > 0 && expected > totalGroupMonths) {
        expected = totalGroupMonths;
    }
    return expected;
}

function calculateDueForMonth(targetMonth, startAmount, schedule = []) {
    if (targetMonth <= 1) return startAmount;
    let currentAmount = startAmount;
    for (let m = 2; m <= targetMonth; m++) {
        let increment = 0;
        for (let tier of schedule) {
            if (m >= tier.start && m <= tier.end) {
                increment = tier.amount;
                break;
            }
        }
        currentAmount += increment;
    }
    return currentAmount;
}

// --- 5. PROFESSIONAL INDIAN PHONE NUMBER SANITIZER ---
// Standardizes phone numbers to Meta E.164 digits format (e.g., 919876543210)
function formatIndianPhoneNumber(rawPhone) {
    if (!rawPhone) return null;
    let digits = String(rawPhone).replace(/\D/g, '');

    if (digits.length === 10) {
        digits = '91' + digits;
    } else if (digits.length === 11 && digits.startsWith('0')) {
        digits = '91' + digits.substring(1);
    } else if (digits.length === 12 && digits.startsWith('91')) {
        // Already valid E.164 format
    } else {
        return null;
    }

    return digits;
}

function chunkArray(array, size) {
    const chunked = [];
    for (let i = 0; i < array.length; i += size) {
        chunked.push(array.slice(i, i + size));
    }
    return chunked;
}

// --- 6. OFFICIAL META CLOUD API CLIENT ENGINE ---

// Raw HTTPS caller for Meta WhatsApp Business Cloud API
function callMetaWhatsAppAPI(payload) {
    return new Promise((resolve, reject) => {
        if (!META_ACCESS_TOKEN || !META_PHONE_NUMBER_ID) {
            return reject(new Error('Meta credentials not configured on server. Check environment variables.'));
        }

        const dataString = JSON.stringify(payload);
        const options = {
            hostname: 'graph.facebook.com',
            port: 443,
            path: `/${META_API_VERSION}/${META_PHONE_NUMBER_ID}/messages`,
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${META_ACCESS_TOKEN}`,
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(dataString)
            }
        };

        const req = https.request(options, (res) => {
            let body = '';
            res.on('data', (chunk) => body += chunk);
            res.on('end', () => {
                try {
                    const parsed = JSON.parse(body);
                    if (res.statusCode >= 200 && res.statusCode < 300) {
                        resolve(parsed);
                    } else {
                        reject(new Error(`Meta API Error (${res.statusCode}): ${parsed.error?.message || body}`));
                    }
                } catch (parseErr) {
                    reject(new Error(`Failed to parse Meta response: ${body}`));
                }
            });
        });

        req.on('error', (e) => reject(e));
        req.write(dataString);
        req.end();
    });
}

// Send standard conversational text message (Valid during active 24-hr customer service windows)
async function sendFreeTextMessage(toPhone, text) {
    const cleanTo = formatIndianPhoneNumber(toPhone);
    if (!cleanTo) throw new Error(`Invalid recipient phone: ${toPhone}`);

    const payload = {
        messaging_product: 'whatsapp',
        recipient_type: 'individual',
        to: cleanTo,
        type: 'text',
        text: { preview_url: false, body: text }
    };

    return await callMetaWhatsAppAPI(payload);
}

// Send Pre-Approved Template Message
async function sendTemplateMessage(toPhone, templateName, components = []) {
    const cleanTo = formatIndianPhoneNumber(toPhone);
    if (!cleanTo) throw new Error(`Invalid recipient phone: ${toPhone}`);

    const payload = {
        messaging_product: 'whatsapp',
        recipient_type: 'individual',
        to: cleanTo,
        type: 'template',
        template: {
            name: templateName,
            language: { code: 'en' },
            components: components
        }
    };

    return await callMetaWhatsAppAPI(payload);
}

// Template Sender: Participant Payment Reminder
async function sendParticipantReminderTemplate(toPhone, { groupName, timeline, participantName, breakdown, totalAmount, userId }) {
    const components = [
        {
            type: 'body',
            parameters: [
                { type: 'text', text: String(groupName) },
                { type: 'text', text: String(timeline) },
                { type: 'text', text: String(participantName) },
                { type: 'text', text: String(breakdown) },
                { type: 'text', text: String(totalAmount) }
            ]
        },
        {
            type: 'button',
            sub_type: 'url',
            index: '0',
            parameters: [
                { type: 'text', text: String(userId) }
            ]
        }
    ];

    try {
        return await sendTemplateMessage(toPhone, 'chit_payment_due_reminder', components);
    } catch (err) {
        console.warn(`[FALLBACK NOTICE] Template 'chit_payment_due_reminder' failed or pending approval (${err.message}). Attempting free-text dispatch fallback.`);
        const fallbackText = 
`*Meena Chitfunds*
Group: ${groupName}
Timeline: Month ${timeline}

Dear ${participantName},
You have pending payments for the following months:

${breakdown}
*Total Pending: ₹${totalAmount}*

Kindly clear your dues at the earliest.

View your ledger & pay here:
https://corporationgoorac.github.io/ChitFunds/#${userId}`;
        return await sendFreeTextMessage(toPhone, fallbackText);
    }
}

// Template Sender: Admin Cycle Start Notification
async function sendAdminCycleStart(adminPhone, cycleName, timestamp, queuedCount) {
    const components = [
        {
            type: 'body',
            parameters: [
                { type: 'text', text: String(cycleName) },
                { type: 'text', text: String(timestamp) },
                { type: 'text', text: String(queuedCount) }
            ]
        }
    ];

    try {
        return await sendTemplateMessage(adminPhone, 'admin_dispatch_started', components);
    } catch (err) {
        const text = `🚀 *Background Auto-Dispatch Started*\nCycle: ${cycleName}\nQueued Groups: ${queuedCount}\nTimestamp: ${timestamp}`;
        return await sendFreeTextMessage(adminPhone, text).catch(() => {});
    }
}

// Template Sender: Admin Group Completion Report
async function sendAdminGroupReport(adminPhone, { groupId, groupName, deliveredCount, failureCount, totalReminded }) {
    const components = [
        {
            type: 'body',
            parameters: [
                { type: 'text', text: String(groupId) },
                { type: 'text', text: String(groupName) },
                { type: 'text', text: String(deliveredCount) },
                { type: 'text', text: String(failureCount) },
                { type: 'text', text: String(totalReminded) }
            ]
        }
    ];

    try {
        return await sendTemplateMessage(adminPhone, 'admin_group_summary_report', components);
    } catch (err) {
        const report = 
`✅ *Dispatch Complete: Group @${groupId}*
Group Name: ${groupName.toUpperCase()}
- Reminders Delivered: ${deliveredCount}
- Delivery Failures: ${failureCount}
- Pending Value Reminded: ₹${totalReminded}

Group struck from active queue.`;
        return await sendFreeTextMessage(adminPhone, report).catch(() => {});
    }
}

// Get Admin Phone Target (Supports dynamic Firebase fetch and fallback targets)
async function getAdminPhoneTarget() {
    try {
        const adminSnap = await db.collection('system_state').doc('admin_settings').get();
        if (adminSnap.exists && adminSnap.data().adminPhone) {
            return formatIndianPhoneNumber(adminSnap.data().adminPhone);
        }
    } catch (err) {
        console.error('[ADMIN FETCH ERROR]', err);
    }

    if (process.env.ADMIN_PHONE) {
        return formatIndianPhoneNumber(process.env.ADMIN_PHONE);
    }
    return null;
}

// --- 7. THE PRIORITY DISPATCH ENGINE ---

// 7.1 Timer that identifies if the automated batch needs to be injected into the queue
// ZERO-READ OPTIMIZATION: Bypasses Firestore entirely if the current window has already executed.
async function checkAndRunAutoDispatch() {
    if (isAutoDispatchPaused || globalCancelFlag) return;
    if (isQuietHours()) return;

    try {
        const now = new Date();
        const istString = now.toLocaleString("en-US", {timeZone: "Asia/Kolkata"});
        const istDate = new Date(istString);
        const day = istDate.getDate();
        const monthYear = `${istDate.getMonth() + 1}-${istDate.getFullYear()}`;

        let currentWindow = day <= 15 ? 'Window1' : 'Window2';
        let fieldName = day <= 15 ? 'lastRunWindow1' : 'lastRunWindow2';
        const expectedWindowKey = `${fieldName}_${monthYear}`;

        // STEP 1: ZERO-READ IN-MEMORY SHORT-CIRCUIT
        // If memory verifies this window already ran, stop immediately without touching Firestore (0 Reads)
        if (cachedWindowKey === expectedWindowKey) {
            return;
        }

        // STEP 2: COLD-START VALIDATION (1 Read on first boot or when switching windows)
        const stateSnap = await AUTO_STATE_REF.get();
        const state = stateSnap.exists ? stateSnap.data() : {};

        // If Firestore confirms this cycle was previously completed, cache in memory and exit immediately
        if (state[fieldName] === monthYear) {
            cachedWindowKey = expectedWindowKey;
            
            // Check if unfinished groups remained before server restart
            const pending = state.pendingGroups || [];
            const activeGroup = state.currentProcessingGroup;
            if (activeGroup || pending.length > 0) {
                triggerMasterQueue();
            }
            return;
        }

        // STEP 3: DISPATCH NEW CYCLE (Window has officially switched)
        console.log(`[AUTO-DISPATCH] Triggering new cycle for ${currentWindow} (${monthYear})`);
        const groupsSnap = await db.collection('groups').get();
        const allGroupIds = groupsSnap.docs.map(d => d.id);
        
        await AUTO_STATE_REF.set({
            [fieldName]: monthYear,
            pendingGroups: allGroupIds
        }, { merge: true });

        // Lock in-memory cache to prevent subsequent hourly checks from executing Firestore reads
        cachedWindowKey = expectedWindowKey;

        const adminPhone = await getAdminPhoneTarget();
        if (adminPhone) {
            await sendAdminCycleStart(
                adminPhone, 
                `${currentWindow} (${monthYear})`, 
                istDate.toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }), 
                `${allGroupIds.length} groups`
            );
        }
        
        // Spin up the engine
        triggerMasterQueue();
    } catch (err) {
        console.error(`[AUTO-DISPATCH ERROR] Failed to check state:`, err);
    }
}

// 7.2 The Master Execution Engine (Auto takes Priority over Manual)
async function triggerMasterQueue() {
    if (isDispatching || isAutoDispatchPaused || globalCancelFlag) return;
    
    isDispatching = true; // Engage Global Lock
    let groupsProcessedThisSession = 0;

    try {
        console.log('[MASTER ENGINE] Scanning queues...');
        
        while (!globalCancelFlag && !isAutoDispatchPaused) {
            if (isQuietHours()) {
                console.log('[MASTER ENGINE] Quiet hours active. Pausing engine until 6:00 AM.');
                break;
            }

            // PRIORITY 1: AUTO QUEUE
            let autoState = (await AUTO_STATE_REF.get()).data() || {};
            let autoPending = autoState.pendingGroups || [];
            let autoCurrent = autoState.currentProcessingGroup;

            if (autoCurrent || autoPending.length > 0) {
                if (groupsProcessedThisSession > 0) {
                    console.log(`[MASTER ENGINE] Initiating 1m inter-group settle cooldown (AUTO)...`);
                    let interrupted = await interruptibleWaitMinutes(1, 1, 'GROUP SETTLE COOLDOWN');
                    if (interrupted) break;
                }
                await processOneGroup('AUTO', AUTO_STATE_REF);
                groupsProcessedThisSession++;
                continue; 
            }

            // PRIORITY 2: MANUAL QUEUE (Only runs if Auto is 100% finished)
            let manualState = (await MANUAL_STATE_REF.get()).data() || {};
            let manualPending = manualState.pendingGroups || [];
            let manualCurrent = manualState.currentProcessingGroup;

            if (manualCurrent || manualPending.length > 0) {
                if (groupsProcessedThisSession > 0) {
                    console.log(`[MASTER ENGINE] Initiating 1m inter-group settle cooldown (MANUAL)...`);
                    let interrupted = await interruptibleWaitMinutes(1, 1, 'GROUP SETTLE COOLDOWN');
                    if (interrupted) break;
                }
                await processOneGroup('MANUAL', MANUAL_STATE_REF);
                groupsProcessedThisSession++;
                continue; 
            }

            // If we reach here, both queues are completely empty
            console.log('[MASTER ENGINE] All queues empty. Entering idle state.');
            if (groupsProcessedThisSession > 0 && !globalCancelFlag && !isAutoDispatchPaused && !isQuietHours()) {
                const adminPhone = await getAdminPhoneTarget();
                if (adminPhone) {
                    const text = `🏁 *Master Engine Idle*\nAll automated and manual queues are completely clear. Everything is done.`;
                    await sendFreeTextMessage(adminPhone, text).catch(() => {});
                }
            }
            break;
        }
    } catch (criticalErr) {
        console.error('[CRITICAL MASTER ENGINE ERROR]:', criticalErr);
    } finally {
        isDispatching = false; // Release Global Lock
    }
}

// 7.3 The Singular Processing Logic (Runs cleanly on whichever Ref is passed to it)
async function processOneGroup(queueType, STATE_REF) {
    const adminPhone = await getAdminPhoneTarget();
    
    try {
        let stateSnap = await STATE_REF.get();
        let state = stateSnap.exists ? stateSnap.data() : {};
        
        let groupId = state.currentProcessingGroup;

        // Claim the next group if one isn't currently locked
        if (!groupId && state.pendingGroups && state.pendingGroups.length > 0) {
            groupId = state.pendingGroups[0];
            await STATE_REF.set({ currentProcessingGroup: groupId }, { merge: true });
        }

        if (!groupId) return; // Failsafe
        console.log(`[${queueType}] Processing Group @${groupId}...`);

        const groupSnap = await db.collection('groups').doc(groupId).get();
        
        if (!groupSnap.exists) {
            // Clean Wipe: Ghost group removed instantly
            await STATE_REF.update({ 
                pendingGroups: admin.firestore.FieldValue.arrayRemove(groupId),
                currentProcessingGroup: admin.firestore.FieldValue.delete(),
                pendingUsers: admin.firestore.FieldValue.delete()
            }).catch(() => {});
            return;
        }

        const groupData = groupSnap.data();
        const memberSnapshot = groupData.memberSnapshot || [];
        const startAmt = groupData.startAmount || 0;
        const schedule = groupData.installmentSchedule || [];
        
        const allUserIds = memberSnapshot.map(m => m.id);
        if (allUserIds.length === 0) {
            await STATE_REF.update({ 
                pendingGroups: admin.firestore.FieldValue.arrayRemove(groupId),
                currentProcessingGroup: admin.firestore.FieldValue.delete(),
                pendingUsers: admin.firestore.FieldValue.delete()
            }).catch(() => {});
            return;
        }

        const idBatches = chunkArray(allUserIds, 30);
        let userRecords = [];
        for (const batch of idBatches) {
            const userSnap = await db.collection('users').where(admin.firestore.FieldPath.documentId(), 'in', batch).get();
            userSnap.forEach(docSnap => { userRecords.push({ id: docSnap.id, ...docSnap.data() }); });
        }

        const snapshotMap = new Map();
        memberSnapshot.forEach(m => snapshotMap.set(m.id, m));

        const dispatchQueue = [];
        let groupPendingTotal = 0;

        for (const user of userRecords) {
            const expectedUserMonth = calculateParticipantExpectedMonth(user, groupData);
            const snap = snapshotMap.get(user.id) || { monthsPaid: 0 };
            const monthsPaid = snap.monthsPaid || 0;
            
            if (monthsPaid < expectedUserMonth) {
                let totalOwed = 0;
                let pendingMonthsList = [];

                for (let m = monthsPaid + 1; m <= expectedUserMonth; m++) {
                    const dueForM = calculateDueForMonth(m, startAmt, schedule);
                    totalOwed += dueForM;
                    pendingMonthsList.push({ month: m, amount: dueForM });
                }

                const targetPhone = formatIndianPhoneNumber(user.phone);
                if (!targetPhone) continue;

                groupPendingTotal += totalOwed;
                let breakdownText = "";
                pendingMonthsList.forEach(pm => { breakdownText += `- Month ${pm.month}: ₹${pm.amount.toLocaleString('en-IN')}\n`; });

                const participantName = (user.name || 'Participant').toUpperCase();
                const groupName = (groupData.groupName || groupId).toUpperCase();

                dispatchQueue.push({ 
                    id: user.id, 
                    name: participantName, 
                    phone: targetPhone, 
                    groupName: groupName,
                    timeline: `${expectedUserMonth} of ${groupData.totalMonths || 0}`,
                    breakdown: breakdownText.trim(),
                    amount: totalOwed.toLocaleString('en-IN')
                });
            }
        }

        // LOCK PENDING USERS (Prevents crash duplicates)
        let currentSysState = await STATE_REF.get();
        let sysData = currentSysState.exists ? currentSysState.data() : {};
        
        if (!sysData.pendingUsers) {
            let allPendingIds = dispatchQueue.map(item => item.id);
            await STATE_REF.set({ pendingUsers: allPendingIds }, { merge: true });
            sysData.pendingUsers = allPendingIds;
        }
        
        let pendingUsers = sysData.pendingUsers || [];
        let filteredDispatchQueue = dispatchQueue.filter(item => pendingUsers.includes(item.id));

        let successCount = 0;
        let failCount = 0;

        // META DISPATCH LOOP (5-10 second safe rate control)
        for (let i = 0; i < filteredDispatchQueue.length; i++) {
            if (globalCancelFlag || isAutoDispatchPaused || isQuietHours()) {
                return; // Break immediately, preserving precise state
            }

            const item = filteredDispatchQueue[i];
            try {
                await sendParticipantReminderTemplate(item.phone, {
                    groupName: item.groupName,
                    timeline: item.timeline,
                    participantName: item.name,
                    breakdown: item.breakdown,
                    totalAmount: item.amount,
                    userId: item.id
                });
                successCount++;
                console.log(`[${queueType}] Template sent to ${item.name} (${item.phone})`);
            } catch (sendErr) {
                console.error(`[DISPATCH ERROR] Failed sending to ${item.phone}:`, sendErr.message);
                failCount++;
            } finally {
                await STATE_REF.update({ pendingUsers: admin.firestore.FieldValue.arrayRemove(item.id) }).catch(() => {});
            }

            if (i < filteredDispatchQueue.length - 1) {
                console.log(`[${queueType}] Pacing dispatch: waiting 5-10 seconds before next recipient...`);
                let interrupted = await interruptibleWaitSeconds(5, 10, 'RECIPIENT PACING');
                if (interrupted) return;
            }
        }

        // VERIFY GROUP COMPLETION
        let postState = await STATE_REF.get();
        let postPending = postState.exists ? (postState.data().pendingUsers || []) : [];
        let remainingActionable = postPending.filter(id => dispatchQueue.some(item => item.id === id));

        if (!globalCancelFlag && !isAutoDispatchPaused && remainingActionable.length === 0) {
            // ZERO JUNK DATA CLEANUP: Hard delete the memory footprint for this group
            await STATE_REF.update({ 
                pendingGroups: admin.firestore.FieldValue.arrayRemove(groupId),
                currentProcessingGroup: admin.firestore.FieldValue.delete(),
                pendingUsers: admin.firestore.FieldValue.delete()
            }).catch(err => console.error("Firebase Finalize Sync Error", err));

            if (adminPhone) {
                await sendAdminGroupReport(adminPhone, {
                    groupId: groupId,
                    groupName: groupData.groupName || groupId,
                    deliveredCount: successCount,
                    failureCount: failCount,
                    totalReminded: groupPendingTotal.toLocaleString('en-IN')
                });
            }
        }
    } catch (err) {
        console.error(`[PROCESS GROUP ERROR]`, err);
        if (adminPhone) {
            await sendFreeTextMessage(adminPhone, `❌ *Server Error during execution:*\n${err.message}`).catch(() => {});
        }
    }
}

// --- 8. MANUAL COMMAND INGESTION HANDLERS ---
async function handleChitfundsDispatch(groupId, requesterPhone) {
    try {
        const groupSnap = await db.collection('groups').doc(groupId).get();
        if (!groupSnap.exists) {
            await sendFreeTextMessage(requesterPhone, `❌ *Group Not Found*\nNo chit group exists with ID: @${groupId}`);
            return;
        }

        await MANUAL_STATE_REF.set({
            pendingGroups: admin.firestore.FieldValue.arrayUnion(groupId)
        }, { merge: true });

        await sendFreeTextMessage(requesterPhone, `⏳ *Queued*\nGroup @${groupId} safely injected into the Manual Queue.\n\n*Note:* The Master Engine respects Priority execution. If Auto-Tasks are running, this group will automatically execute once they finish.`);
        
        triggerMasterQueue(); // Call the engine
    } catch (err) {
        console.error(err);
        await sendFreeTextMessage(requesterPhone, `❌ Error enqueuing command: ${err.message}`).catch(() => {});
    }
}

async function handleGlobalChitfundsDispatch(requesterPhone) {
    try {
        const groupsSnap = await db.collection('groups').get();
        if (groupsSnap.empty) {
            await sendFreeTextMessage(requesterPhone, `✅ No active groups found in database.`);
            return;
        }

        const allGroupIds = groupsSnap.docs.map(d => d.id);
        await MANUAL_STATE_REF.set({
            pendingGroups: admin.firestore.FieldValue.arrayUnion(...allGroupIds)
        }, { merge: true });

        await sendFreeTextMessage(requesterPhone, `🌐 *Global Queue Added*\nAppended all active database groups to the Manual Queue.\n\n*Note:* The Master Engine executes with priority. If system is currently running scheduled auto-tasks, global manual execution will yield until auto-tasks finish.`);
        
        triggerMasterQueue(); // Call the engine
    } catch (err) {
        console.error(err);
        await sendFreeTextMessage(requesterPhone, `❌ Error enqueuing global command: ${err.message}`).catch(() => {});
    }
}

// --- 9. INBOUND WEBHOOK CONTROLLER (META HANDSHAKE & EVENT STREAM) ---

// 9.1 Handshake Verification (GET /webhook)
app.get('/webhook', (req, res) => {
    const mode = req.query['hub.mode'];
    const token = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];

    if (mode === 'subscribe' && token === WEBHOOK_VERIFY_TOKEN) {
        console.log('[WEBHOOK] Verified Meta handshake successfully.');
        res.status(200).send(challenge);
    } else {
        console.warn('[WEBHOOK] Verification failed. Tokens do not match.');
        res.sendStatus(403);
    }
});

// 9.2 Inbound Message Receiver (POST /webhook)
app.post('/webhook', async (req, res) => {
    // Immediate acknowledgement to Meta to avoid retry duplication loops
    res.sendStatus(200);

    const body = req.body;
    if (body.object !== 'whatsapp_business_account') return;
    
    // FETCH ADMIN PHONE ONCE PER PAYLOAD (Optimized outside the nested message loop)
    const currentAdminPhone = await getAdminPhoneTarget();

    try {
        const entries = body.entry || [];
        for (const entry of entries) {
            const changes = entry.changes || [];
            for (const change of changes) {
                const value = change.value;
                if (!value || !value.messages) continue;

                for (const msg of value.messages) {
                    if (msg.type !== 'text') continue;

                    const messageText = (msg.text?.body || '').trim();
                    const senderPhone = formatIndianPhoneNumber(msg.from);
                    console.log(`[INBOUND MESSAGE] Received: "${messageText}" from ${senderPhone}`);

                    // Validate if sender is Authorized Admin
                    const isAuthorized = currentAdminPhone ? (senderPhone === currentAdminPhone) : true;

                    // DYNAMIC ADMIN NUMBER REGISTRATION
                    const adminRegex = /^(?:chitfunds\s+)?change admin number\s+(.+)$/i;
                    const adminMatch = messageText.match(adminRegex);
                    if (adminMatch && adminMatch[1]) {
                        if (!isAuthorized && currentAdminPhone) {
                            console.warn(`[SECURITY] Unauthorized admin change attempt from ${senderPhone}`);
                            return;
                        }
                        const rawPhone = adminMatch[1].trim();
                        const formatted = formatIndianPhoneNumber(rawPhone);
                        if (formatted) {
                            await db.collection('system_state').doc('admin_settings').set({ adminPhone: formatted }, { merge: true });
                            await sendFreeTextMessage(senderPhone, `✅ Admin number successfully updated to +${formatted}. All future reports will be routed here.`);
                        } else {
                            await sendFreeTextMessage(senderPhone, `❌ Invalid phone number format. Please provide a valid 10-digit number.`);
                        }
                        return;
                    }

                    if (!isAuthorized) {
                        console.warn(`[ACCESS DENIED] Ignoring command from non-admin phone: ${senderPhone}`);
                        return;
                    }

                    // EMERGENCY KILL-SWITCH
                    if (messageText.toLowerCase() === 'chitfunds stop') {
                        globalCancelFlag = true;
                        isAutoDispatchPaused = true;
                        
                        // Hard-delete manual queue on stop to prevent it from ghost-resuming later
                        await MANUAL_STATE_REF.set({
                            pendingGroups: [],
                            currentProcessingGroup: admin.firestore.FieldValue.delete(),
                            pendingUsers: admin.firestore.FieldValue.delete()
                        }, { merge: true });

                        await sendFreeTextMessage(senderPhone, `🛑 *System Halted*\nManual queue cleared. Automated queue paused. Existing tasks are breaking out of loops safely.`);
                        return;
                    }

                    // PAUSE / RESUME BACKGROUND AUTO-SENDER
                    if (messageText.toLowerCase() === 'chitfunds pause') {
                        isAutoDispatchPaused = true;
                        await sendFreeTextMessage(senderPhone, `⏸ *System Paused*\nThe Master Queue will not execute any groups until resumed.`);
                        return;
                    }
                    if (messageText.toLowerCase() === 'chitfunds resume') {
                        isAutoDispatchPaused = false;
                        globalCancelFlag = false;
                        if (isQuietHours()) {
                            await sendFreeTextMessage(senderPhone, `▶️ *System Resumed*\nHowever, Quiet Hours (11 PM - 6 AM) are active. Master Queue will run at 6:00 AM.`);
                        } else {
                            await sendFreeTextMessage(senderPhone, `▶️ *System Resumed*\nChecking Master Queue priorities...`);
                            triggerMasterQueue(); // Kick off the master engine
                        }
                        return;
                    }

                    // GLOBAL DISPATCH TRIGGER (MANUAL)
                    if (messageText.toLowerCase() === 'chitfunds all' || messageText.toLowerCase() === 'auto chitfunds all') {
                        console.log(`[TRIGGER] Received GLOBAL Chitfunds command from ${senderPhone}`);
                        globalCancelFlag = false;
                        await handleGlobalChitfundsDispatch(senderPhone);
                        return;
                    }

                    // SINGLE GROUP DISPATCH TRIGGER (MANUAL)
                    const triggerRegex = /^chitfunds\s+([A-Za-z0-9_-]+)/i;
                    const match = messageText.match(triggerRegex);

                    if (!match) return;

                    const targetGroupId = match[1].trim();
                    console.log(`[TRIGGER] Received Chitfunds command for group: "${targetGroupId}" from ${senderPhone}`);
                    globalCancelFlag = false;
                    await handleChitfundsDispatch(targetGroupId, senderPhone);
                }
            }
        }
    } catch (webhookErr) {
        console.error('[WEBHOOK PROCESSOR ERROR]:', webhookErr);
    }
});

// --- 10. EXPRESS DASHBOARD & STATUS API ---
app.get('/api/status', (req, res) => {
    res.json({
        status: connectionStatus,
        user: connectedUser,
        phoneId: META_PHONE_NUMBER_ID || 'Not Configured',
        dispatchActive: isDispatching,
        autoPaused: isAutoDispatchPaused
    });
});

app.get('/', (req, res) => {
    res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>WhatsApp Gateway - Meena Chitfunds</title>
  <style>
    :root {
      --bg-color: #f4f7f6;
      --text-main: #111111;
      --text-muted: #6b7280;
      --surface-color: #ffffff;
      --surface-hover: #f9fafb;
      --border-color: #e5e7eb;
      --brand-accent: #065fd4;
      --brand-accent-light: #e6f0fa;
      --danger-color: #dc3545;
      --danger-light: #fde8e8;
      --warning-color: #f59e0b;
      --warning-light: #fef3c7;
      --success-color: #0f9d58;
      --success-light: #e6f4ea;
      --card-shadow: 0 4px 12px rgba(0,0,0,0.03), 0 1px 3px rgba(0,0,0,0.02);
    }
    @media (prefers-color-scheme: dark) {
      :root {
        --bg-color: #0a0a0a;
        --text-main: #f8f9fa;
        --text-muted: #9ca3af;
        --surface-color: #141414;
        --surface-hover: #1f1f1f;
        --border-color: #2d2d2d;
        --brand-accent: #3ea6ff;
        --brand-accent-light: rgba(62, 166, 255, 0.15);
        --danger-color: #ff4e45;
        --danger-light: rgba(255, 78, 69, 0.15);
        --warning-color: #fbbf24;
        --warning-light: rgba(251, 191, 36, 0.15);
        --success-color: #34a853;
        --success-light: rgba(52, 168, 83, 0.15);
        --card-shadow: 0 4px 12px rgba(0,0,0,0.2);
      }
    }
    * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
    body {
      background-color: var(--bg-color);
      color: var(--text-main);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      margin: 0; padding: 0;
      display: flex; flex-direction: column; align-items: center; justify-content: center;
      min-height: 100vh;
    }
    .container {
      width: 90%; max-width: 440px; background: var(--surface-color);
      border: 1px solid var(--border-color); border-radius: 24px; padding: 32px 24px;
      box-shadow: var(--card-shadow); text-align: center;
    }
    .status-badge {
      display: inline-block; padding: 6px 14px; border-radius: 20px;
      font-size: 11px; font-weight: 800; text-transform: uppercase; letter-spacing: 1px;
      margin-bottom: 24px;
    }
    .status-badge.connected { background: var(--success-light); color: var(--success-color); }
    .status-badge.waiting { background: var(--warning-light); color: var(--warning-color); }
    .status-badge.offline { background: var(--danger-light); color: var(--danger-color); }
    .status-icon-box {
      width: 100px; height: 100px; margin: 0 auto 20px;
      border: 1px solid var(--border-color); border-radius: 50%;
      display: flex; align-items: center; justify-content: center;
      background: var(--surface-hover);
    }
    .title { font-size: 20px; font-weight: 800; margin: 0 0 8px 0; }
    .subtitle { font-size: 13px; color: var(--text-muted); font-weight: 500; margin: 0 0 24px 0; line-height: 1.4; }
    .info-card {
      background: var(--surface-hover); border: 1px solid var(--border-color);
      border-radius: 12px; padding: 12px; font-size: 12px; margin-bottom: 20px; text-align: left;
    }
    .info-card div { margin-bottom: 4px; }
    .info-card span { font-weight: 700; color: var(--brand-accent); }
    .command-box {
      background: var(--bg-color); border: 1px solid var(--border-color);
      border-radius: 12px; padding: 12px; font-family: monospace; font-size: 13px;
      font-weight: 700; color: var(--brand-accent); word-break: break-all;
    }
  </style>
</head>
<body>
  <div class="container">
    <div id="badge" class="status-badge waiting">Checking Meta Cloud Gateway...</div>
    <div class="status-icon-box" id="icon-box">
      <svg viewBox="0 0 24 24" width="48" height="48" fill="var(--brand-accent)"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 14H9v-2h2v2zm0-4H9V7h2v5z"/></svg>
    </div>
    <h2 class="title">WhatsApp Cloud Gateway</h2>
    <p class="subtitle" id="instruction">Connected directly to Official Meta Business Graph APIs.</p>

    <div class="info-card">
      <div>Webhook Status: <span id="webhook-status">Active (/webhook)</span></div>
      <div>Phone Number ID: <span id="phone-id">${META_PHONE_NUMBER_ID || 'Not Configured'}</span></div>
      <div>Pacing Strategy: <span>5-10s Member / 1m Group</span></div>
    </div>

    <div style="text-align: left; margin-top: 16px;">
      <div style="font-size: 11px; text-transform: uppercase; font-weight: 800; color: var(--text-muted); margin-bottom: 6px;">Trigger Syntax</div>
      <div class="command-box" style="margin-bottom: 8px;">Chitfunds &lt;groupId&gt; <span style="float:right; font-size: 11px; color: var(--text-muted);">Single Group</span></div>
      <div class="command-box" style="margin-bottom: 8px;">Chitfunds all <span style="float:right; font-size: 11px; color: var(--text-muted);">Global Auto-Run</span></div>
      <div class="command-box">Chitfunds stop <span style="float:right; font-size: 11px; color: var(--danger-color);">Kill Switch</span></div>
    </div>
  </div>

  <script>
    async function syncStatus() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();

        const badge = document.getElementById('badge');
        const iconBox = document.getElementById('icon-box');
        const instruction = document.getElementById('instruction');

        if (data.status === 'connected') {
          badge.className = 'status-badge connected';
          badge.innerText = 'Meta API Online';
          instruction.innerText = 'Official Meta Cloud API webhook listener is operational.';
          iconBox.innerHTML = '<svg viewBox="0 0 24 24" width="48" height="48" fill="var(--success-color)"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/></svg>';
        } else {
          badge.className = 'status-badge offline';
          badge.innerText = 'Missing Secrets';
          instruction.innerText = 'Configure META_ACCESS_TOKEN and META_PHONE_NUMBER_ID in secrets.';
          iconBox.innerHTML = '<svg viewBox="0 0 24 24" width="48" height="48" fill="var(--danger-color)"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/></svg>';
        }
      } catch (err) {
        console.error(err);
      }
    }

    setInterval(syncStatus, 5000);
    syncStatus();
  </script>
</body>
</html>
    `);
});

// --- 11. START SERVER & BACKGROUND INITIALIZATION ---
server.listen(PORT, () => {
    console.log(`[HTTP] Official WhatsApp Cloud Gateway running on http://0.0.0.0:${PORT}`);
    console.log(`[HTTP] Webhook Verification Route: GET /webhook`);
    console.log(`[HTTP] Webhook Inbound Message Route: POST /webhook`);

    // Initialize the automated cron runner
    if (!autoDispatchInterval) {
        autoDispatchInterval = setInterval(checkAndRunAutoDispatch, 60 * 60 * 1000);
        checkAndRunAutoDispatch(); 
    }
});
