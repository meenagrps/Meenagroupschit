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
 * 16. Integrated GST Reporter Engine with Native Cloud PDF Uploader.
 */

const express = require('express');
const http = require('http');
const https = require('https');
const { initializeApp, cert, getApps } = require('firebase-admin/app');
const { getFirestore, FieldValue, FieldPath } = require('firebase-admin/firestore');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { performance } = require('perf_hooks');

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

if (getApps().length === 0) {
    initializeApp({
        credential: cert(serviceAccount)
    });
}

const db = getFirestore();

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
let cachedAdminPhone = null; // ZERO-READ RAM CACHE FOR ADMIN PHONE

// ZERO-READ IN-MEMORY CYCLE CACHE
let cachedWindowKey = null;

// GST GLOBAL STATE
const HF_API_URL = "https://corporationgoorac-quanai.hf.space/api/generate-bot";
let isManualGenerating = false;
const systemTelemetry = { requestsHandled: 0, lastError: null, bootTime: Date.now() };

// Delay Helpers
const wait = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function interruptibleWaitSeconds(minSec, maxSec, type = 'DELAY') {
    const ms = Math.floor(Math.random() * ((maxSec * 1000) - (minSec * 1000) + 1)) + (minSec * 1000);
    const intervals = Math.floor(ms / 1000);
    for (let i = 0; i < intervals; i++) {
        if (globalCancelFlag || isAutoDispatchPaused) {
            console.log(`[${type}] Sleep interrupted by system flag.`);
            return true;
        }
        await wait(1000);
    }
    return false;
}

async function interruptibleWaitMinutes(min, max, type = 'DELAY') {
    const ms = Math.floor(Math.random() * ((max * 60000) - (min * 60000) + 1)) + (min * 60000);
    const intervals = Math.floor(ms / 5000);
    for (let i = 0; i < intervals; i++) {
        if (globalCancelFlag || isAutoDispatchPaused) {
            console.log(`[${type}] Sleep interrupted by system flag.`);
            return true;
        }
        await wait(5000);
    }
    return false;
}

// Quiet Hours Helper (11:00 PM to 6:00 AM IST)
function isQuietHours() {
    const istString = new Date().toLocaleString("en-US", {timeZone: "Asia/Kolkata"});
    const istHour = new Date(istString).getHours();
    return istHour >= 23 || istHour < 6;
}

// Fetch Helper (Node 26 native)
async function fetchWithTimeout(resource, options = {}) {
    const { timeout = 300000 } = options;
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), timeout);
    try {
        const response = await fetch(resource, { ...options, signal: controller.signal });
        clearTimeout(id);
        return response;
    } catch (error) {
        clearTimeout(id);
        throw error;
    }
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

// NATIVE MULTIPART MEDIA UPLOADER (NO EXTERNAL PACKAGES)
function uploadMediaToMetaNative(buffer, filename, mimeType = 'application/pdf') {
    return new Promise((resolve, reject) => {
        if (!META_ACCESS_TOKEN || !META_PHONE_NUMBER_ID) {
            return reject(new Error('Meta credentials not configured.'));
        }

        const boundary = '----WhatsAppMediaBoundary' + Date.now().toString(16);
        let postDataStart = Buffer.from(
            `--${boundary}\r\n` +
            `Content-Disposition: form-data; name="messaging_product"\r\n\r\n` +
            `whatsapp\r\n` +
            `--${boundary}\r\n` +
            `Content-Disposition: form-data; name="file"; filename="${filename}"\r\n` +
            `Content-Type: ${mimeType}\r\n\r\n`
        );
        let postDataEnd = Buffer.from(`\r\n--${boundary}--\r\n`);
        
        let postDataLength = postDataStart.length + buffer.length + postDataEnd.length;

        const options = {
            hostname: 'graph.facebook.com',
            port: 443,
            path: `/${META_API_VERSION}/${META_PHONE_NUMBER_ID}/media`,
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${META_ACCESS_TOKEN}`,
                'Content-Type': `multipart/form-data; boundary=${boundary}`,
                'Content-Length': postDataLength
            }
        };

        const req = https.request(options, (res) => {
            let body = '';
            res.on('data', (chunk) => body += chunk);
            res.on('end', () => {
                try {
                    const parsed = JSON.parse(body);
                    if (res.statusCode >= 200 && res.statusCode < 300 && parsed.id) {
                        resolve(parsed.id);
                    } else {
                        reject(new Error(`Meta Media Upload Error (${res.statusCode}): ${parsed.error?.message || body}`));
                    }
                } catch (parseErr) {
                    reject(new Error(`Failed to parse Meta media response: ${body}`));
                }
            });
        });

        req.on('error', (e) => reject(e));
        req.write(postDataStart);
        req.write(buffer);
        req.write(postDataEnd);
        req.end();
    });
}

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

async function sendDocumentMessage(toPhone, mediaId, filename, caption = "") {
    const cleanTo = formatIndianPhoneNumber(toPhone);
    if (!cleanTo) throw new Error(`Invalid recipient phone: ${toPhone}`);

    const payload = {
        messaging_product: 'whatsapp',
        recipient_type: 'individual',
        to: cleanTo,
        type: 'document',
        document: {
            id: mediaId,
            filename: filename,
            caption: caption
        }
    };
    return await callMetaWhatsAppAPI(payload);
}

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

async function sendGSTTemplateMessage(toPhone, mediaId, filename, reportingMonthStr, templateName = 'gst_monthly_report') {
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
            components: [
                {
                    type: 'header',
                    parameters: [
                        {
                            type: 'document',
                            document: {
                                id: mediaId,
                                filename: filename
                            }
                        }
                    ]
                },
                {
                    type: 'body',
                    parameters: [
                        { type: 'text', text: String(reportingMonthStr) }
                    ]
                }
            ]
        }
    };
    return await callMetaWhatsAppAPI(payload);
}

async function sendGSTComplianceTemplate(toPhone, missingBillsText, templateName = 'gst_compliance_alert') {
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
            components: [
                {
                    type: 'body',
                    parameters: [
                        { type: 'text', text: String(missingBillsText) }
                    ]
                }
            ]
        }
    };
    return await callMetaWhatsAppAPI(payload);
}

async function getAdminPhoneTarget() {
    if (cachedAdminPhone) {
        return cachedAdminPhone;
    }

    try {
        const adminSnap = await db.collection('system_state').doc('admin_settings').get();
        if (adminSnap.exists && adminSnap.data().adminPhone) {
            cachedAdminPhone = formatIndianPhoneNumber(adminSnap.data().adminPhone);
            return cachedAdminPhone;
        }
    } catch (err) {
        console.error('[ADMIN FETCH ERROR]', err);
    }
    
    if (process.env.ADMIN_PHONE) {
        cachedAdminPhone = formatIndianPhoneNumber(process.env.ADMIN_PHONE);
        return cachedAdminPhone;
    }
    return null;
}

// --- 7. THE PRIORITY DISPATCH ENGINE ---
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

        if (cachedWindowKey === expectedWindowKey) return;

        const stateSnap = await AUTO_STATE_REF.get();
        const state = stateSnap.exists ? stateSnap.data() : {};

        if (state[fieldName] === monthYear) {
            cachedWindowKey = expectedWindowKey;
            const pending = state.pendingGroups || [];
            const activeGroup = state.currentProcessingGroup;
            if (activeGroup || pending.length > 0) {
                triggerMasterQueue();
            }
            return;
        }

        console.log(`[AUTO-DISPATCH] Triggering new cycle for ${currentWindow} (${monthYear})`);
        const groupsSnap = await db.collection('groups').get();
        const allGroupIds = groupsSnap.docs.map(d => d.id);
        
        await AUTO_STATE_REF.set({
            [fieldName]: monthYear,
            pendingGroups: allGroupIds
        }, { merge: true });

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
        
        triggerMasterQueue();
    } catch (err) {
        console.error(`[AUTO-DISPATCH ERROR] Failed to check state:`, err);
    }
}

async function triggerMasterQueue() {
    if (isDispatching || isAutoDispatchPaused || globalCancelFlag) return;
    
    isDispatching = true;
    let groupsProcessedThisSession = 0;

    try {
        console.log('[MASTER ENGINE] Scanning queues...');
        
        while (!globalCancelFlag && !isAutoDispatchPaused) {
            if (isQuietHours()) {
                console.log('[MASTER ENGINE] Quiet hours active. Pausing engine until 6:00 AM.');
                break;
            }

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
        isDispatching = false; 
    }
}

async function processOneGroup(queueType, STATE_REF) {
    const adminPhone = await getAdminPhoneTarget();
    
    try {
        let stateSnap = await STATE_REF.get();
        let state = stateSnap.exists ? stateSnap.data() : {};
        
        let groupId = state.currentProcessingGroup;

        if (!groupId && state.pendingGroups && state.pendingGroups.length > 0) {
            groupId = state.pendingGroups[0];
            await STATE_REF.set({ currentProcessingGroup: groupId }, { merge: true });
        }

        if (!groupId) return; 
        console.log(`[${queueType}] Processing Group @${groupId}...`);

        const groupSnap = await db.collection('groups').doc(groupId).get();
        
        if (!groupSnap.exists) {
            await STATE_REF.update({ 
                pendingGroups: FieldValue.arrayRemove(groupId),
                currentProcessingGroup: FieldValue.delete(),
                pendingUsers: FieldValue.delete()
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
                pendingGroups: FieldValue.arrayRemove(groupId),
                currentProcessingGroup: FieldValue.delete(),
                pendingUsers: FieldValue.delete()
            }).catch(() => {});
            return;
        }

        const idBatches = chunkArray(allUserIds, 30);
        let userRecords = [];
        for (const batch of idBatches) {
            const userSnap = await db.collection('users').where(FieldPath.documentId(), 'in', batch).get();
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

        for (let i = 0; i < filteredDispatchQueue.length; i++) {
            if (globalCancelFlag || isAutoDispatchPaused || isQuietHours()) {
                return; 
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
                await STATE_REF.update({ pendingUsers: FieldValue.arrayRemove(item.id) }).catch(() => {});
            }

            if (i < filteredDispatchQueue.length - 1) {
                console.log(`[${queueType}] Pacing dispatch: waiting 5-10 seconds before next recipient...`);
                let interrupted = await interruptibleWaitSeconds(5, 10, 'RECIPIENT PACING');
                if (interrupted) return;
            }
        }

        let postState = await STATE_REF.get();
        let postPending = postState.exists ? (postState.data().pendingUsers || []) : [];
        let remainingActionable = postPending.filter(id => dispatchQueue.some(item => item.id === id));

        if (!globalCancelFlag && !isAutoDispatchPaused && remainingActionable.length === 0) {
            await STATE_REF.update({ 
                pendingGroups: FieldValue.arrayRemove(groupId),
                currentProcessingGroup: FieldValue.delete(),
                pendingUsers: FieldValue.delete()
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
            pendingGroups: FieldValue.arrayUnion(groupId)
        }, { merge: true });

        await sendFreeTextMessage(requesterPhone, `⏳ *Queued*\nGroup @${groupId} safely injected into the Manual Queue.\n\n*Note:* The Master Engine respects Priority execution. If Auto-Tasks are running, this group will automatically execute once they finish.`);
        
        triggerMasterQueue();
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
            pendingGroups: FieldValue.arrayUnion(...allGroupIds)
        }, { merge: true });

        await sendFreeTextMessage(requesterPhone, `🌐 *Global Queue Added*\nAppended all active database groups to the Manual Queue.\n\n*Note:* The Master Engine executes with priority. If system is currently running scheduled auto-tasks, global manual execution will yield until auto-tasks finish.`);
        
        triggerMasterQueue();
    } catch (err) {
        console.error(err);
        await sendFreeTextMessage(requesterPhone, `❌ Error enqueuing global command: ${err.message}`).catch(() => {});
    }
}

// --- 8.5 GST REPORTER LOGIC ---
async function handleManualGSTCommand(senderPhone, match) {
    if (isManualGenerating) {
        await sendFreeTextMessage(senderPhone, "⏳ I am already generating a GST report. Please wait a moment.");
        return;
    }

    isManualGenerating = true;
    systemTelemetry.requestsHandled++;
    const _perfStart = performance.now();

    const isForce = !!match[1];
    const param = match[2] ? match[2].trim() : null;

    try {
        await sendFreeTextMessage(senderPhone, `⏳ _Connecting to Hugging Face Cloud Engine..._\n_Please wait while the PDF is assembled._`);

        let payload = { force: isForce };
        if (param) {
            payload.mode = 'sequence';
            payload.sequenceNo = param;
        } else {
            payload.mode = 'date';
            const istNow = new Date(new Date().toLocaleString("en-US", {timeZone: "Asia/Kolkata"}));
            const y = istNow.getFullYear();
            const m = String(istNow.getMonth() + 1).padStart(2, '0');
            const lastDay = new Date(y, istNow.getMonth() + 1, 0).getDate();
            payload.fromDate = `${y}-${m}-01`;
            payload.toDate = `${y}-${m}-${lastDay}`;
        }

        console.log(`[SYS-ADVANCED] Outbound API Payload: ${JSON.stringify(payload)}`);

        const response = await fetchWithTimeout(HF_API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            if (response.status === 400) {
                const errorData = await response.json();
                let errorMsg = `⚠️ *COMPLIANCE ERROR DETECTED*\nI halted the PDF generation to prevent tax sequence violations.\n\n`;
                if (errorData.missing && errorData.missing.length > 0) errorMsg += `❌ *Missing Bills:* ${errorData.missing.join(', ')}\n`;
                if (errorData.duplicates && errorData.duplicates.length > 0) errorMsg += `⚠️ *Duplicates:* ${errorData.duplicates.join(', ')}\n`;
                errorMsg += `\n_Reply with *Force GST report ${param || ''}* to bypass this safety check._`;
                await sendFreeTextMessage(senderPhone, errorMsg.trim());
                return;
            } else {
                throw new Error(`Server returned status: ${response.status}`);
            }
        }

        const arrayBuffer = await response.arrayBuffer();
        let pdfBuffer = Buffer.from(arrayBuffer);
        
        const filename = `GST_Report_${Date.now()}.pdf`;
        const caption = isForce 
            ? "⚠️ *FORCED GST REPORT*\n_This document contains known sequence anomalies._" 
            : "✅ *GST REPORT GENERATED*\n_Strict sequence validation passed._";
        
        const mediaId = await uploadMediaToMetaNative(pdfBuffer, filename, 'application/pdf');
        await sendDocumentMessage(senderPhone, mediaId, filename, caption);

        const _perfEnd = performance.now();
        await sendFreeTextMessage(senderPhone, `⚡ _Cloud Generation & Secure Transmission completed in ${((_perfEnd - _perfStart) / 1000).toFixed(2)} seconds._`);

        pdfBuffer = null;
        if (global.gc) global.gc();

    } catch (error) {
        console.error("Manual GST Error:", error);
        let errMsg = error.name === 'AbortError' 
            ? "⏳ *Timeout Error:* The cloud server took too long to wake up. Please try again." 
            : `❌ *Error:* Failed to generate PDF. (${error.message})`;
        await sendFreeTextMessage(senderPhone, errMsg);
    } finally {
        isManualGenerating = false;
    }
}

async function runAutomatedGSTCheck(isBootUp = false) {
    try {
        const istNow = new Date(new Date().toLocaleString("en-US", {timeZone: "Asia/Kolkata"}));
        let targetYear = istNow.getFullYear();
        let targetMonth = istNow.getMonth(); 
        
        const isLastDay = new Date(targetYear, targetMonth + 1, 0).getDate() === istNow.getDate();
        const isLateNight = istNow.getHours() >= 22; 
        
        if (!isBootUp && !(isLastDay && isLateNight)) {
            return;
        }

        let reportingMonthStr = "";
        let isCatchup = false;

        if (isLastDay && isLateNight) {
            reportingMonthStr = `${targetYear}-${String(targetMonth + 1).padStart(2, '0')}`;
        } else {
            let prevMonthDate = new Date(targetYear, targetMonth - 1, 1);
            reportingMonthStr = `${prevMonthDate.getFullYear()}-${String(prevMonthDate.getMonth() + 1).padStart(2, '0')}`;
            isCatchup = true;
        }

        const docRef = db.collection('gst_reporter').doc('status');
        const docSnap = await docRef.get();
        let data = docSnap.exists ? docSnap.data() : { currentMonth: "", status: "IDLE", updatedAt: 0 };
        
        if (data.currentMonth === reportingMonthStr && data.status === "SENT") return; 

        const lastUpdated = data.updatedAt ? new Date(data.updatedAt) : new Date(0);
        const minutesSinceUpdate = (istNow - lastUpdated) / (1000 * 60);

        if (data.currentMonth === reportingMonthStr && data.status === "PROCESSING") {
            if (minutesSinceUpdate < 15) return; 
            console.log("[SYS] Stale PROCESSING state detected for GST. Assuming crash. Overriding...");
        }

        await docRef.set({
            currentMonth: reportingMonthStr,
            status: "PROCESSING",
            updatedAt: istNow.toISOString()
        });

        const adminPhone = await getAdminPhoneTarget();
        if (!adminPhone) {
            console.error("[GST] No admin phone configured. Aborting automated run.");
            return;
        }

        const [yStr, mStr] = reportingMonthStr.split('-');
        const y = parseInt(yStr);
        const m = parseInt(mStr);
        const lastDayOfTarget = new Date(y, m, 0).getDate();
        
        const payload = {
            mode: 'date',
            fromDate: `${yStr}-${mStr}-01`,
            toDate: `${yStr}-${mStr}-${lastDayOfTarget}`,
            force: false 
        };

        const response = await fetchWithTimeout(HF_API_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            if (response.status === 400) {
                const errorData = await response.json();
                
                let errorDetailsList = [];
                if (errorData.missing && errorData.missing.length > 0) {
                    errorDetailsList.push(`Missing: ${errorData.missing.join(', ')}`);
                }
                if (errorData.duplicates && errorData.duplicates.length > 0) {
                    errorDetailsList.push(`Duplicates: ${errorData.duplicates.join(', ')}`);
                }
                
                let combinedErrorStr = errorDetailsList.length > 0 ? errorDetailsList.join(' | ') : "Multiple sequence errors";
                
                try {
                    await sendGSTComplianceTemplate(adminPhone, combinedErrorStr, 'gst_compliance_alert');
                } catch (tempErr) {
                    let fallback = `🚨 *AUTOMATED GST REPORT FAILED*\nCompliance errors found in sequence for ${reportingMonthStr}.\nDetails: ${combinedErrorStr}`;
                    await sendFreeTextMessage(adminPhone, fallback).catch(()=>{});
                }
                
                await docRef.set({ currentMonth: reportingMonthStr, status: "FAILED", updatedAt: new Date().toISOString() });
                return;
            } else {
                throw new Error(`Server returned status: ${response.status}`);
            }
        }

        const arrayBuffer = await response.arrayBuffer();
        let pdfBuffer = Buffer.from(arrayBuffer);
        const filename = `GST_Report_${reportingMonthStr}.pdf`;
        
        const mediaId = await uploadMediaToMetaNative(pdfBuffer, filename, 'application/pdf');

        try {
            await sendGSTTemplateMessage(adminPhone, mediaId, filename, reportingMonthStr, 'gst_monthly_report');
        } catch (tempErr) {
            console.warn(`[FALLBACK NOTICE] Template 'gst_monthly_report' failed. Attempting free-text document fallback.`);
            const caption = isCatchup 
                ? `✅ *RECOVERED GST REPORT*\n_This report for ${reportingMonthStr} was missed during an outage and has been automatically recovered._`
                : `✅ *MONTHLY GST REPORT*\n_Automated delivery for ${reportingMonthStr}._`;
            await sendDocumentMessage(adminPhone, mediaId, filename, caption);
        }

        pdfBuffer = null;
        if (global.gc) global.gc();

        await docRef.set({
            currentMonth: reportingMonthStr,
            status: "SENT",
            updatedAt: new Date().toISOString()
        });
        console.log(`[SYS] Automated GST Report for ${reportingMonthStr} successfully delivered.`);

    } catch (error) {
        console.error("Automated GST System Error:", error);
        const adminPhone = await getAdminPhoneTarget();
        if (adminPhone && error.name !== 'AbortError') {
            await sendFreeTextMessage(adminPhone, `❌ *Automated GST Task Error:* Failed to process report. (${error.message})`).catch(()=>{});
        }
        const docRef = db.collection('gst_reporter').doc('status');
        await docRef.set({
            status: "FAILED",
            updatedAt: new Date().toISOString()
        }, { merge: true });
    }
}

// --- 9. INBOUND WEBHOOK CONTROLLER (META HANDSHAKE & EVENT STREAM) ---
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

app.post('/webhook', async (req, res) => {
    res.sendStatus(200);

    const body = req.body;
    if (body.object !== 'whatsapp_business_account') return;
    
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

                    const isAuthorized = currentAdminPhone ? (senderPhone === currentAdminPhone) : true;

                    // ADVANCED SYS PING
                    if (messageText.toLowerCase() === 'gst sys ping') {
                        if (!isAuthorized) return;
                        const mem = process.memoryUsage();
                        const uptime = ((Date.now() - systemTelemetry.bootTime) / 60000).toFixed(2);
                        const load = os.loadavg()[0].toFixed(2);
                        const reply = `⚙️ *ADVANCED SYSTEM DIAGNOSTICS*\n\n` +
                            `⏱️ *Uptime:* ${uptime} min\n` +
                            `🧠 *RAM (Heap):* ${(mem.heapUsed / 1024 / 1024).toFixed(2)} MB\n` +
                            `🖥️ *CPU Load (1m):* ${load}\n` +
                            `🔒 *Process Lock:* ${isManualGenerating ? 'ACTIVE (BUSY)' : 'IDLE (READY)'}\n` +
                            `📈 *Reports Served:* ${systemTelemetry.requestsHandled}`;
                        await sendFreeTextMessage(senderPhone, reply);
                        return;
                    }

                    // GST MANUAL COMMANDS
                    const gstMatch = messageText.match(/^(force\s+)?gst\s+report(?:\s+(.+))?$/i);
                    if (gstMatch) {
                        if (!isAuthorized) {
                            console.warn(`[SECURITY] Unauthorized GST attempt from ${senderPhone}`);
                            return;
                        }
                        await handleManualGSTCommand(senderPhone, gstMatch);
                        return;
                    }

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
                            cachedAdminPhone = formatted; // UPDATE RAM CACHE INSTANTLY
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

                    if (messageText.toLowerCase() === 'chitfunds stop') {
                        globalCancelFlag = true;
                        isAutoDispatchPaused = true;
                        
                        await MANUAL_STATE_REF.set({
                            pendingGroups: [],
                            currentProcessingGroup: FieldValue.delete(),
                            pendingUsers: FieldValue.delete()
                        }, { merge: true });

                        await sendFreeTextMessage(senderPhone, `🛑 *System Halted*\nManual queue cleared. Automated queue paused. Existing tasks are breaking out of loops safely.`);
                        return;
                    }

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
                            triggerMasterQueue(); 
                        }
                        return;
                    }

                    if (messageText.toLowerCase() === 'chitfunds all' || messageText.toLowerCase() === 'auto chitfunds all') {
                        console.log(`[TRIGGER] Received GLOBAL Chitfunds command from ${senderPhone}`);
                        globalCancelFlag = false;
                        await handleGlobalChitfundsDispatch(senderPhone);
                        return;
                    }

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
      <div class="command-box" style="margin-bottom: 8px;">gst report <span style="float:right; font-size: 11px; color: var(--text-muted);">GST Fetch</span></div>
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

    // Fetch and cache the Admin phone into RAM immediately on boot
    getAdminPhoneTarget().then(phone => {
        console.log(`[SYS] Admin phone cached on startup: +${phone || 'Not Configured'}`);
    });

    // Initialize Chit Funds Cron
    if (!autoDispatchInterval) {
        autoDispatchInterval = setInterval(checkAndRunAutoDispatch, 60 * 60 * 1000);
        checkAndRunAutoDispatch(); 
    }

    // Initialize GST Checkers
    setInterval(() => runAutomatedGSTCheck(false), 600000); 
    setTimeout(() => runAutomatedGSTCheck(true), 45000); 
});
