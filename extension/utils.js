/**
 * Utility functions for Hardcover Sync Extension
 */

export const Utils = {
    // Port of thefuzz's token_sort_ratio basic approximation or standard levenshtein
    // Since we just need a decent fuzzy match, Levenshtein Distance is usually enough.
    // We normalize simple token sort by sorting words first.
    tokenSortRatio: (str1, str2) => {
        if (!str1 || !str2) return 0;
        
        // 1. Tokenize & Sort
        const s1 = str1.toLowerCase().replace(/[^\w\s]/g, '').split(/\s+/).sort().join(" ");
        const s2 = str2.toLowerCase().replace(/[^\w\s]/g, '').split(/\s+/).sort().join(" ");
        
        if (s1 === s2) return 100;
        
        // 2. Levenshtein
        const lev = Utils.levenshtein(s1, s2);
        const maxLen = Math.max(s1.length, s2.length);
        
        return Math.floor((1 - lev / maxLen) * 100);
    },

    levenshtein: (a, b) => {
        if (a.length === 0) return b.length;
        if (b.length === 0) return a.length;
        
        const matrix = [];
        for (let i = 0; i <= b.length; i++) matrix[i] = [i];
        for (let j = 0; j <= a.length; j++) matrix[0][j] = j;
        
        for (let i = 1; i <= b.length; i++) {
            for (let j = 1; j <= a.length; j++) {
                if (b.charAt(i - 1) === a.charAt(j - 1)) {
                    matrix[i][j] = matrix[i - 1][j - 1];
                } else {
                    matrix[i][j] = Math.min(
                        matrix[i - 1][j - 1] + 1, // substitution
                        Math.min(
                            matrix[i][j - 1] + 1, // insertion
                            matrix[i - 1][j] + 1  // deletion
                        )
                    );
                }
            }
        }
        return matrix[b.length][a.length];
    },

    // XML Parser for RSS
    parseRSS: (xmlText) => {
        const parser = new DOMParser();
        const xmlDoc = parser.parseFromString(xmlText, "text/xml");
        const items = xmlDoc.querySelectorAll("item");
        const entries = [];

        items.forEach(item => {
            const getTag = (tag) => item.getElementsByTagName(tag)[0]?.textContent;
            
            entries.push({
                title: getTag("title"),
                author_name: getTag("author_name"),
                isbn: getTag("isbn"),
                isbn13: getTag("isbn13"),
                user_rating: getTag("user_rating"),
                user_read_at: getTag("user_read_at"),
                user_date_added: getTag("user_date_added"),
                book_id: getTag("book_id") // Goodread ID
            });
        });
        return entries;
    },

    log: (msg, type='info', containerId='log-container') => {
        console.log(`[${type.toUpperCase()}] ${msg}`);
        const container = document.getElementById(containerId);
        if (container) {
            const div = document.createElement('div');
            div.className = `log-${type}`;
            div.textContent = `[${new Date().toLocaleTimeString()}] ${msg}`;
            container.appendChild(div);
            container.scrollTop = container.scrollHeight;
        }
    }
};
