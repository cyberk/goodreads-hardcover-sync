/**
 * Hardcover Sync Extension Logic
 */

const HC_ENDPOINT = "https://api.hardcover.app/v1/graphql";
let HC_TOKEN = null;
let RSS_URL = null;

// DOM Elements
const btnSync = document.getElementById('btn-sync');
const statusGoodreads = document.getElementById('status-goodreads');
const statusHardcover = document.getElementById('status-hardcover');

// --- Initialization ---

document.addEventListener('DOMContentLoaded', async () => {
    Utils.log("Initializing...", "debug");
    
    // 1. Try to get Credentials from Chrome Storage first
    const storage = await chrome.storage.local.get(['hc_token', 'rss_url']);
    if (storage.hc_token) HC_TOKEN = storage.hc_token;
    if (storage.rss_url) RSS_URL = storage.rss_url;

    // 2. Discover Credentials if missing
    if (!HC_TOKEN) await discoverHardcoverToken();
    else setStatus(statusHardcover, "Ready", true);

    if (!RSS_URL) await discoverGoodreadsRSS();
    else setStatus(statusGoodreads, "Ready", true);

    // 3. Enable Sync Button
    if (HC_TOKEN && RSS_URL) {
        btnSync.disabled = false;
    }
});

btnSync.addEventListener('click', async () => {
    btnSync.disabled = true;
    btnSync.textContent = "Syncing...";
    try {
        await runSync();
        btnSync.textContent = "Sync Complete!";
    } catch (e) {
        Utils.log(`Sync Failed: ${e.message}`, "error");
        btnSync.textContent = "Sync Failed";
    } finally {
        setTimeout(() => {
            btnSync.disabled = false;
            if (btnSync.textContent === "Sync Complete!") btnSync.textContent = "Sync Now";
        }, 3000);
    }
});

function setStatus(el, text, isGood) {
    el.textContent = text;
    el.className = `status-value ${isGood ? 'connected' : 'error'}`;
}

// --- Credential Discovery ---

async function discoverHardcoverToken() {
    Utils.log("Checking Hardcover session...", "debug");
    try {
        const response = await fetch("https://hardcover.app/account/api");
        if (response.url.includes("login")) {
            setStatus(statusHardcover, "Login Required", false);
            Utils.log("Please log in to Hardcover.app in a new tab.", "error");
            return;
        }

        const html = await response.text();
        // Parsing logic: Find the token in the code blocks
        // It's usually in a <code> block or input. Simple regex might work better on the full HTML
        // Looking for the long bearer token string
        const match = html.match(/eyJ[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+/);
        
        if (match) {
            HC_TOKEN = match[0];
            await chrome.storage.local.set({ hc_token: HC_TOKEN });
            setStatus(statusHardcover, "Detected", true);
            Utils.log("Reference found for Hardcover Token.", "success");
        } else {
            console.log("HTML Preview:", html.substring(0, 500)); // Debug
            setStatus(statusHardcover, "Not Found", false);
            Utils.log("Could not find API Token on account page.", "error");
        }
    } catch (e) {
        setStatus(statusHardcover, "Error", false);
        Utils.log(`Hardcover Check Error: ${e.message}`, "error");
    }
}

async function discoverGoodreadsRSS() {
    Utils.log("Checking Goodreads session...", "debug");
    try {
        // Fetch "Read" shelf
        const response = await fetch("https://www.goodreads.com/review/list?shelf=read");
        if (response.url.includes("user/sign_in")) {
            setStatus(statusGoodreads, "Login Required", false);
            Utils.log("Please log in to Goodreads.com in a new tab.", "error");
            return;
        }
        
        const html = await response.text();
        
        // Use DOM Parser for robust finding
        const parser = new DOMParser();
        const doc = parser.parseFromString(html, "text/html");
        
        // Find any link containing 'list_rss'
        // Usually located in footer or bottom of list
        const rssLink = doc.querySelector('a[href*="/review/list_rss/"]');
        
        if (rssLink) {
            let href = rssLink.getAttribute('href');
            
            // Fix relative URLs
            if (href.startsWith('/')) {
                href = `https://www.goodreads.com${href}`;
            }
            
            // Ensure shelf=read is in there, if not, append or warn?
            // Usually the page 'shelf=read' generates a 'shelf=read' RSS link.
            // But let's actally trust the link found on the page.
            
            RSS_URL = href;
            await chrome.storage.local.set({ rss_url: RSS_URL });
            setStatus(statusGoodreads, "Detected", true);
            Utils.log("Reference found for Goodreads RSS.", "success");
        } else {
            setStatus(statusGoodreads, "Not Found", false);
            Utils.log("Could not find RSS link on Goodreads page. Make sure you are logged in.", "error");
        }

    } catch (e) {
        setStatus(statusGoodreads, "Error", false);
        Utils.log(`Goodreads Check Error: ${e.message}`, "error");
    }
}

// --- Sync Logic (Ported from sync.py) ---

async function graphqlQuery(query, variables) {
    const authHeader = HC_TOKEN.startsWith("Bearer ") ? HC_TOKEN : `Bearer ${HC_TOKEN}`;
    const res = await fetch(HC_ENDPOINT, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': authHeader
        },
        body: JSON.stringify({ query, variables })
    });
    
    if (!res.ok) throw new Error(`API Error: ${res.statusText}`);
    const json = await res.json();
    if (json.errors) throw new Error(`GraphQL Error: ${JSON.stringify(json.errors)}`);
    return json;
}

async function getHardcoverLibraryIds() {
    const query = `
    query GetMyBooks {
      me {
        user_books(where: {status_id: {_eq: 3}}) {
          book {
            id
            title
            editions {
              isbn_10
              isbn_13
            }
          }
        }
      }
    }
    `;
    try {
        const res = await graphqlQuery(query);
        const bookIds = new Set();
        const existingIsbns = new Set();
        const existingTitles = new Set();
        
        const userBooks = res.data.me[0].user_books;
        userBooks.forEach(ub => {
            const book = ub.book;
            bookIds.add(book.id);
            existingTitles.add(book.title.trim().toLowerCase());
            
            if (book.editions) {
                book.editions.forEach(ed => {
                    if (ed.isbn_10) existingIsbns.add(ed.isbn_10);
                    if (ed.isbn_13) existingIsbns.add(ed.isbn_13);
                });
            }
        });
        return { bookIds, existingIsbns, existingTitles };
    } catch (e) {
        Utils.log(`Library Fetch Error: ${e.message}`, "warn");
        return { bookIds: new Set(), existingIsbns: new Set(), existingTitles: new Set() };
    }
}

async function searchHardcoverBookId(title, author, isbn) {
    // "Winner Takes All" Logic Port
    const candidates = {}; // map ID -> object

    // Helper: Search and Verify
    const searchAndVerify = async (searchTitle, sourceLabel) => {
        // Utils.log(`Searching ${sourceLabel}: ${searchTitle}`, 'debug');
        const query = `
        query SearchBooks($title: String!) {
          books(where: {title: {_eq: $title}}, limit: 50, order_by: {users_count: desc}) {
            id
            title
            users_count
            contributions {
              author { name }
            }
          }
        }
        `;
        try {
            const res = await graphqlQuery(query, { title: searchTitle });
            const books = res.data.books || [];
            
            books.forEach(bk => {
                let authors = [];
                if (bk.contributions) {
                    bk.contributions.forEach(c => {
                        if (c.author && c.author.name) authors.push(c.author.name);
                    });
                }
                
                if (authors.length === 0) return;

                // Fuzzy Check
                let isMatch = false;
                for (let ba of authors) {
                    if (Utils.tokenSortRatio(author, ba) > 70) {
                        isMatch = true;
                        break;
                    }
                }

                if (isMatch) {
                    if (!candidates[bk.id]) {
                        bk.match_source = sourceLabel;
                        candidates[bk.id] = bk;
                    }
                }
            });

        } catch (e) {
            console.error(e);
        }
    };

    // 1. ISBN Search
    if (isbn) {
        const query = `
        query SearchByISBN($isbn:String!) {
          editions(where: {_or: [{isbn_10: {_eq: $isbn}}, {isbn_13: {_eq: $isbn}}]}) {
            book {
              id
              title
              users_count
            }
          }
        }
        `;
        try {
            const res = await graphqlQuery(query, { isbn });
            const editions = res.data.editions || [];
            editions.forEach(ed => {
                if (ed.book) {
                    const bk = ed.book;
                    if (!candidates[bk.id]) {
                        bk.match_source = "ISBN";
                        candidates[bk.id] = bk;
                    }
                }
            });
        } catch (e) { console.error(e); }
    }

    // 2. Full Title Search
    await searchAndVerify(title.trim(), "FullTitle");

    // 3. Short Title Search
    const separators = [':', '(', '-'];
    const checkedShort = new Set();
    
    // We run these sequentially or Promise.all - sequential is safer for rate limits but slower
    // Parallel is fine since browser handles it.
    const promises = [];
    separators.forEach(sep => {
        if (title.includes(sep)) {
            const shortTitle = title.split(sep)[0].trim();
            if (shortTitle.length >= 4 && !checkedShort.has(shortTitle)) {
                checkedShort.add(shortTitle);
                promises.push(searchAndVerify(shortTitle, `ShortTitle(${sep})`));
            }
        }
    });
    await Promise.all(promises);

    // 4. Decision
    const finalList = Object.values(candidates);
    if (finalList.length === 0) return null;

    // Sort by popularity desc
    finalList.sort((a, b) => (b.users_count || 0) - (a.users_count || 0));

    const winner = finalList[0];
    Utils.log(`Match: '${winner.title}' (Src: ${winner.match_source}, Users: ${winner.users_count})`, "debug");
    
    return winner.id;
}

async function addBookToHardcover(bookId, rating, readDate) {
    const mutation = `
    mutation AddUserBook($book_id: Int!, $rating: numeric) {
      insert_user_book(object: {
        book_id: $book_id, 
        status_id: 3, 
        rating: $rating
      }) {
        id
        error
      }
    }
    `;
    // Clean rating (Goodreads is 0-5, Hardcover expects similar?)
    // Hardcover uses 0-5.
    const parsedRating = rating ? parseInt(rating) : null;
    
    try {
        const res = await graphqlQuery(mutation, { book_id: bookId, rating: parsedRating });
        const retData = res.data.insert_user_book;
        if (retData && retData.error) {
            Utils.log(`Failed to add: ${retData.error}`, "error");
            return null;
        }
        return retData ? retData.id : null;
    } catch (e) {
        if (e.message.includes("Uniqueness violation")) {
            Utils.log("Book already in library (API check).", "warn");
        } else {
            Utils.log(`Error adding book: ${e.message}`, "error");
        }
        return null;
    }
}

async function addReadDate(userBookId, readDate) {
    const mutation = `
    mutation AddReadDate($user_book_id: Int!, $finished_at: date) {
      insert_user_book_read(user_book_id: $user_book_id, user_book_read: {finished_at: $finished_at}) {
        id
      }
    }
    `;
    try {
        await graphqlQuery(mutation, { user_book_id: userBookId, finished_at: readDate });
        Utils.log(`+ Added Read Date: ${readDate}`, "success");
    } catch (e) {
        Utils.log(`Failed to add date: ${e.message}`, "error");
    }
}

async function runSync() {
    Utils.log("=== Starting Sync ===", "info");
    
    // 1. Fetch RSS
    Utils.log("Fetching RSS Feed...", "info");
    const res = await fetch(RSS_URL);
    const text = await res.text();
    const entries = Utils.parseRSS(text);
    
    if (entries.length === 0) {
        Utils.log("No books found in RSS feed.", "warn");
        return;
    }

    // 2. Fetch Library (Cache)
    Utils.log("Fetching Library Cache...", "info");
    const { bookIds, existingIsbns, existingTitles } = await getHardcoverLibraryIds();
    Utils.log(`Found ${bookIds.size} existing books.`, "info");

    // Process most recent 10 (hardcoded for safety for now? or unlimited)
    // Let's do 20 for safety in Extension
    const limit = 20;
    const processList = entries.slice(0, limit).reverse(); // Oldest first

    let addedCount = 0;

    for (const entry of processList) {
        // --- Checks ---
        // 1. ISBN Check
        if (entry.isbn13 && existingIsbns.has(entry.isbn13)) {
            // Utils.log(`Skipping '${entry.title}' (ISBN match)`, "debug");
            continue;
        }
        // 2. Title Check
        if (existingTitles.has(entry.title.trim().toLowerCase())) {
            // Utils.log(`Skipping '${entry.title}' (Title match)`, "debug");
            continue;
        }

        // --- New Book ---
        Utils.log(`Processing: ${entry.title}`, "info");

        // Parse Date
        let readDate = null;
        if (entry.user_read_at) {
             const d = new Date(entry.user_read_at);
             if (!isNaN(d)) readDate = d.toISOString().split('T')[0];
        }

        const bookId = await searchHardcoverBookId(entry.title, entry.author_name, entry.isbn13 || entry.isbn);
        
        if (bookId) {
            // Double check ID
            if (bookIds.has(bookId)) {
                 Utils.log(`-> Already in library (ID check).`, "warn");
                 continue;
            }

            // ADD
            Utils.log(`-> Adding to Hardcover...`, "debug");
            const userBookId = await addBookToHardcover(bookId, entry.user_rating, readDate);
            
            if (userBookId) {
                Utils.log(`-> Success!`, "success");
                bookIds.add(bookId); // Update local cache
                addedCount++;
                
                if (readDate) {
                    await addReadDate(userBookId, readDate);
                }
            }
        } else {
            Utils.log(`-> No match found.`, "warn");
        }
        
        // Slight delay to be nice
        await new Promise(r => setTimeout(r, 500));
    }
    
    Utils.log(`=== Sync Complete. Added ${addedCount} books. ===`, "success");
}
