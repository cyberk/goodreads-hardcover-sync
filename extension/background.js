import { Utils } from './utils.js';

// --- Constants & State ---
const HC_ENDPOINT = "https://api.hardcover.app/v1/graphql";
let HC_TOKEN = null;
let RSS_URL = null;

// --- Listeners ---

// 1. Tab Update (Navigation)
chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
    if (changeInfo.status === 'complete' && tab.url && tab.url.includes('hardcover.app')) {
        checkAndNotify(tabId);
    }
});

// 2. Message from Content Script
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === "START_SYNC") {
        runSync(sender.tab.id, false); // Real Run
    }
});

// --- Core Logic ---

async function checkAndNotify(tabId) {
    console.log("Checking for updates...");
    
    // 1. Credentials
    await loadCredentials();
    console.log(`Credentials Loaded? Token: ${!!HC_TOKEN}, RSS: ${!!RSS_URL}`);
    
    if (!HC_TOKEN || !RSS_URL) {
        console.warn("Credentials missing in background. Can't sync.");
        return; 
    } 

    // 2. Dry Run Check
    console.log("Starting Dry Run...");
    const newBooksCount = await runSync(tabId, true); // Dry Run
    console.log(`Dry Run Complete. Found ${newBooksCount} new books.`);
    
    if (newBooksCount > 0) {
        console.log("Sending SHOW_MODAL message...");
        try {
            chrome.tabs.sendMessage(tabId, {
                action: 'SHOW_MODAL',
                data: { newCount: newBooksCount }
            });
            console.log("Message sent.");
        } catch (e) {
            console.error("Failed to send message:", e);
        }
    } else {
        console.log("No new books found, staying silent.");
    }
}

async function loadCredentials() {
    const storage = await chrome.storage.local.get(['hc_token', 'rss_url']);
    if (storage.hc_token) HC_TOKEN = storage.hc_token;
    if (storage.rss_url) RSS_URL = storage.rss_url;
}

// Reused Discovery logic? 
// For background worker, we can't easily document.querySelector on the active tab 
// without scripting.executeScript.
// For now, let's assume credentials are set via the Popup or cached.
async function discoverHardcoverToken() {
    // If we are mostly relying on cache, we skip this complexity for the background check
    // unless we specifically want to scrape it in the background.
    // Let's keep it simple: if no credentials, don't nag.
    return;
}


// --- Sync Engine (Refactored for Background) ---
// Returns number of new books found (Dry Run) OR runs the add logic
// --- Sync Engine (Refactored for Background) ---
// Returns number of new books found (Dry Run) OR runs the add logic
async function runSync(tabId, isDryRun) {
    try {
        // 1. Fetch RSS
        const res = await fetch(RSS_URL);
        const text = await res.text();
        const entries = Utils.parseRSS(text);
        if (entries.length === 0) return 0;

        // 2. Fetch Library
        const { bookIds, existingIsbns, existingTitles } = await getHardcoverLibraryIds();
        
        // 3. Compare
        const limit = 20;
        const processList = entries.slice(0, limit).reverse();
        
        let newBooksFound = 0;
        console.log(`Processing ${processList.length} recent books from RSS...`);
        
        for (const entry of processList) {
            // --- A. First Pass: Cache Check ---
            if (entry.isbn13 && existingIsbns.has(entry.isbn13)) {
                console.log(`[Skip] '${entry.title}' (ISBN Cache Hit)`);
                continue;
            }
            if (existingTitles.has(entry.title.trim().toLowerCase())) {
                 console.log(`[Skip] '${entry.title}' (Title Cache Hit)`);
                 continue;
            }

            // Fuzzy Check
            let isFuzzyMatch = false;
            for (const existingTitle of existingTitles) {
                if (Utils.tokenSortRatio(entry.title, existingTitle) > 90) {
                    console.log(`[Skip] '${entry.title}' (Fuzzy Cache Hit: '${existingTitle}')`);
                    isFuzzyMatch = true;
                    break;
                }
            }
            if (isFuzzyMatch) continue;

            // --- B. Second Pass: API Verification ---
            console.log(`[Candidate] '${entry.title}' - Verifying via API...`);
            if (tabId && !isDryRun) {
                 chrome.tabs.sendMessage(tabId, { action: 'UPDATE_LOG', message: `Found candidate: ${entry.title}...`, type: 'info' });
            }

            let bookId = null;
            try {
                bookId = await searchHardcoverBookId(entry.title, entry.author_name, entry.isbn13 || entry.isbn);
            } catch (e) {
                console.error("Search Error:", e);
            }

            if (!bookId) {
                console.log(`[No Match] Could not find '${entry.title}' in Hardcover.`);
                if (isDryRun) {
                    // Do NOT count unmatchable books (Fixed)
                } else if (tabId) {
                    chrome.tabs.sendMessage(tabId, { action: 'UPDATE_LOG', message: `⚠️ No match: ${entry.title}`, type: 'warn' });
                }
                continue;
            }

            // --- C. Third Pass: ID Check (The Truth) ---
            if (bookIds.has(bookId)) {
                console.log(`[False Positive] '${entry.title}' resolved to ID ${bookId}, which is already in library.`);
                continue;
            }

            // --- D. Action ---
            console.log(`[Verified New] '${entry.title}' (ID: ${bookId})`);
            
            if (isDryRun) {
                newBooksFound++;
                continue;
            }

            // REAL RUN: Add it
            const userBookId = await addBookToHardcover(bookId, entry.user_rating, entry.user_read_at);
            if (userBookId) {
                bookIds.add(bookId); // Update local set
                if (tabId) chrome.tabs.sendMessage(tabId, { action: 'UPDATE_LOG', message: `✅ Added: ${entry.title}`, type: 'success' });
                
                // Handle Date
                if (entry.user_read_at) {
                    const d = new Date(entry.user_read_at);
                    if (!isNaN(d)) {
                        await addReadDate(userBookId, d.toISOString().split('T')[0]);
                    }
                }
            } else {
                 if (tabId) chrome.tabs.sendMessage(tabId, { action: 'UPDATE_LOG', message: `❌ Failed to add: ${entry.title}`, type: 'error' });
            }
            
            // Rate Limit Protection (Inside Loop)
            await new Promise(r => setTimeout(r, 1000));
        }
        
        if (!isDryRun && tabId) {
            chrome.tabs.sendMessage(tabId, { action: 'SYNC_COMPLETE' });
        }
        
        return newBooksFound;
        
    } catch (e) {
        console.error(e);
        return 0;
    }
}

async function graphqlQuery(query, variables, retries = 3) {
    const authHeader = HC_TOKEN.startsWith("Bearer ") ? HC_TOKEN : `Bearer ${HC_TOKEN}`;
    
    try {
        const res = await fetch(HC_ENDPOINT, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': authHeader },
            body: JSON.stringify({ query, variables })
        });
        
        // Handle Rate Limiting (429)
        if (res.status === 429) {
            if (retries > 0) {
                console.warn(`[API] Throttled (429). Waiting 3s... (Retries left: ${retries})`);
                await new Promise(r => setTimeout(r, 3000));
                return graphqlQuery(query, variables, retries - 1);
            } else {
                throw new Error("API Error 429: Throttled (Max retries reached)");
            }
        }

        if (!res.ok) {
            const text = await res.text();
            console.error(`API Error ${res.status}: ${res.statusText}`, text);
            throw new Error(`API Error ${res.status}: ${res.statusText}`);
        }
        
        const json = await res.json();
        if (json.errors) {
            console.error("GraphQL Errors:", json.errors);
            throw new Error("GraphQL Error: " + JSON.stringify(json.errors));
        }
        return json;

    } catch (e) {
        // Network errors?
        if (retries > 0 && e.message.includes("Failed to fetch")) {
             console.warn(`[API] Network Error. Waiting 2s...`);
             await new Promise(r => setTimeout(r, 2000));
             return graphqlQuery(query, variables, retries - 1);
        }
        throw e;
    }
}

// ... (Copy getHardcoverLibraryIds, searchHardcoverBookId, addBookToHardcover from popup.js)
// For brevity, assuming we will fill these in via replace or a shared file in next step.
// I will implement the FULL content in the next step or put it all here? 
// I'll put the stub here and fill it via next tool call to be safe with size? 
// No, let's try to put the critical ones here.

async function getHardcoverLibraryIds() {
    const query = `query GetMyBooks { me { user_books(where: {status_id: {_eq: 3}}) { book { id title editions { isbn_10 isbn_13 } } } } }`;
    const res = await graphqlQuery(query);
    const bookIds = new Set();
    const existingIsbns = new Set();
    const existingTitles = new Set();
    res.data.me[0].user_books.forEach(ub => {
        bookIds.add(ub.book.id);
        existingTitles.add(ub.book.title.trim().toLowerCase());
        if (ub.book.editions) ub.book.editions.forEach(ed => {
            if (ed.isbn_10) existingIsbns.add(ed.isbn_10);
            if (ed.isbn_13) existingIsbns.add(ed.isbn_13);
        });
    });
    return { bookIds, existingIsbns, existingTitles };
}

async function searchHardcoverBookId(title, author, isbn) {
    const candidates = {};
    const searchAndVerify = async (searchTitle, sourceLabel) => {
        console.log(`[Search] ${sourceLabel}: '${searchTitle}'`);
        const query = `query SearchBooks($title: String!) { books(where: {title: {_eq: $title}}, limit: 50, order_by: {users_count: desc}) { id title users_count contributions { author { name } } } }`;
        const res = await graphqlQuery(query, { title: searchTitle });
        (res.data.books || []).forEach(bk => {
             let authors = (bk.contributions || []).map(c => c.author?.name).filter(n => n);
             if (authors.some(ba => Utils.tokenSortRatio(author, ba) > 70)) {
                 if (!candidates[bk.id]) candidates[bk.id] = { ...bk, match_source: sourceLabel };
             }
        });
    };

    if (isbn) {
         const query = `query SearchByISBN($isbn:String!) { editions(where: {_or: [{isbn_10: {_eq: $isbn}}, {isbn_13: {_eq: $isbn}}]}) { book { id title users_count } } }`;
         const res = await graphqlQuery(query, { isbn });
         (res.data.editions || []).forEach(ed => {
             if (ed.book && !candidates[ed.book.id]) candidates[ed.book.id] = { ...ed.book, match_source: 'ISBN' };
         });
    }

    await searchAndVerify(title.trim(), "FullTitle");
    const separators = [':', '(', '-'];
    for (const sep of separators) {
        if (title.includes(sep)) {
            const short = title.split(sep)[0].trim();
            if (short.length >= 4) await searchAndVerify(short, `ShortTitle(${sep})`);
        }
    }

    const finalist = Object.values(candidates).sort((a, b) => (b.users_count || 0) - (a.users_count || 0));
    return finalist.length ? finalist[0].id : null;
}

async function addBookToHardcover(bookId, rating, readAt) {
    const mutation = `mutation AddUserBook($book_id: Int!, $rating: numeric) { insert_user_book(object: { book_id: $book_id, status_id: 3, rating: $rating }) { id } }`;
    const res = await graphqlQuery(mutation, { book_id: bookId, rating: rating ? parseInt(rating) : null });
    return res.data.insert_user_book?.id;
}

async function addReadDate(userBookId, finishedAt) {
    const mutation = `mutation AddReadDate($user_book_id: Int!, $finished_at: date) { insert_user_book_read(user_book_id: $user_book_id, user_book_read: {finished_at: $finished_at}) { id } }`;
    await graphqlQuery(mutation, { user_book_id: userBookId, finished_at: finishedAt });
}
