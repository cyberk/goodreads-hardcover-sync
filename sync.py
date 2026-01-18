import os
import sys
import argparse
import logging
import time
import feedparser
import requests
from email.utils import parsedate_to_datetime
from datetime import datetime
from dotenv import load_dotenv
from thefuzz import fuzz

# --- CONFIGURATION ---
load_dotenv()

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

RSS_URL = os.environ.get('GOODREADS_RSS_URL')
HC_TOKEN = os.environ.get('HARDCOVER_API_TOKEN')
HC_ENDPOINT = "https://api.hardcover.app/v1/graphql"

# Token Check
if HC_TOKEN:
    logger.info(f"Loaded Hardcover Token: {HC_TOKEN[:15]}... (Length: {len(HC_TOKEN)})")
else:
    logger.error("Hardcover Token is EMPTY or None")

if not RSS_URL or not HC_TOKEN:
    logger.error("Missing Environment Variables. Please set GOODREADS_RSS_URL and HARDCOVER_API_TOKEN.")
    sys.exit(1)

# --- HELPER FUNCTIONS ---

def graphql_query(query, variables=None):
    """Sends a request to the Hardcover API."""
    # Smart Authorization Header
    auth_header = HC_TOKEN
    if not auth_header.startswith("Bearer "):
        auth_header = f"Bearer {auth_header}"

    headers = {
        "Authorization": auth_header,
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(
            HC_ENDPOINT, 
            json={'query': query, 'variables': variables}, 
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        
        res_json = response.json()
        if 'errors' in res_json:
            logger.error(f"GraphQL Errors: {res_json['errors']}")
            raise Exception(f"GraphQL Error: {res_json['errors']}")
        return res_json
        
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error: {e}")
        if response.content:
            logger.error(f"Response Body: {response.text}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"API Request failed: {e}")
        raise

def get_hardcover_library_ids():
    """
    Fetches details of books already in your library to avoid re-querying API.
    Returns: (set of book_ids, set of isbns, set of titles)
    """
    query = """
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
    """
    try:
        result = graphql_query(query)
        book_ids = set()
        existing_isbns = set()
        existing_titles = set()
        
        user_books = result['data']['me'][0]['user_books']
        for ub in user_books:
            book = ub['book']
            book_ids.add(book['id'])
            existing_titles.add(book['title'].strip().lower())
            
            # Collect ISBNs from all editions of this book
            if book.get('editions'):
                for ed in book['editions']:
                    if ed.get('isbn_10'): existing_isbns.add(ed['isbn_10'])
                    if ed.get('isbn_13'): existing_isbns.add(ed['isbn_13'])
                    
        return book_ids, existing_isbns, existing_titles
    except Exception as e:
        logger.warning(f"Could not fetch existing Hardcover library or library is empty. Error: {e}")
        return set(), set(), set()

def search_hardcover_book_id(title, author, isbn=None):
    """
    Searches Hardcover for a book ID using a "Winner Takes All" strategy.
    
    1. Identify candidates from 3 sources:
       - ISBN Match (via Editions)
       - Full Title Match (Exact)
       - Short Title Match (Split by ':', '(', '-')
    2. All candidates must pass Author Verification (>70 fuzzy score).
    3. Compare Valid Candidates:
       - Pick the one with the highest user_count.
       - Logic handles "The Doorman: A Novel" (3 users) vs "The Doorman" (32 users).
    """
    candidates = {} # Map ID -> {data} to deduplicate
    
    # --- 1. ISBN Search ---
    if isbn:
        query = """
        query SearchByISBN($isbn:String!) {
          editions(where: {_or: [{isbn_10: {_eq: $isbn}}, {isbn_13: {_eq: $isbn}}]}) {
            book {
              id
              title
              users_count
            }
          }
        }
        """
        try:
            res = graphql_query(query, {'isbn': isbn})
            editions = res.get('data', {}).get('editions', [])
            
            # Collect all books linked to this ISBN
            for ed in editions:
                if ed.get('book'):
                    bk = ed['book']
                    # ISBN matches are implicitly author-verified by the nature of ISBNs, 
                    # but technically we could check. For now, trust the ISBN linkage 
                    # but store it as a candidate comparison.
                    if bk['id'] not in candidates:
                        bk['match_source'] = 'ISBN'
                        candidates[bk['id']] = bk
        except Exception as e:
            logger.error(f"Search error (ISBN): {e}")

    # --- 2. Title Search (Full) ---
    # We define a helper to search and verify authors
    def search_and_verify(search_title, source_label):
        logger.debug(f"Searching for {source_label}: '{search_title}'")
        query = """
        query SearchBooks($title: String!) {
          books(where: {title: {_eq: $title}}, limit: 50, order_by: {users_count: desc}) {
            id
            title
            users_count
            contributions {
              author {
                name
              }
            }
          }
        }
        """
        try:
            res = graphql_query(query, {"title": search_title})
            found_books = res.get('data', {}).get('books', [])
            
            for bk in found_books:
                # AUTHOR VERIFICATION
                book_authors = []
                if bk.get('contributions'):
                    for c in bk['contributions']:
                        if c.get('author') and c['author'].get('name'):
                            book_authors.append(c['author']['name'])
                
                is_author_match = False
                if not book_authors:
                    continue # Skip if no author data
                
                for ba in book_authors:
                    score = fuzz.token_sort_ratio(author.lower(), ba.lower())
                    if score > 70:
                        is_author_match = True
                        break
                
                if is_author_match:
                    # Valid Candidate
                    if bk['id'] not in candidates:
                         bk['match_source'] = source_label
                         candidates[bk['id']] = bk
                    else:
                        # Update source label to indicate multiple matches found it? 
                        # Or just leave as 'ISBN' if it was already found there.
                        pass
        except Exception as e:
            logger.error(f"Search error ({source_label}): {e}")

    # Run Full Title Search
    search_and_verify(title.strip(), "FullTitle")

    # --- 3. Short Title Search ---
    # Try all separators
    checked_short_titles = set()
    for sep in [':', '(', '-']:
        if sep in title:
            short_title = title.split(sep)[0].strip()
            if len(short_title) < 4: continue
            if short_title in checked_short_titles: continue
            
            search_and_verify(short_title, f"ShortTitle({sep})")
            checked_short_titles.add(short_title)

    # --- 4. Decision Time ---
    if not candidates:
        return None

    # Convert to list
    final_list = list(candidates.values())
    
    # Sort by users_count descending
    final_list.sort(key=lambda x: x.get('users_count') or 0, reverse=True)
    
    winner = final_list[0]
    
    # Logging the decision
    log_msg = f"Match Decision for '{title}': Selected '{winner['title']}' (ID: {winner['id']}, Users: {winner.get('users_count')}, Source: {winner['match_source']})"
    
    # If there were other candidates, log them for debugging
    if len(final_list) > 1:
        others = [f"{c['title']} (ID:{c['id']}, U:{c.get('users_count')}, S:{c['match_source']})" for c in final_list[1:]]
        logger.info(f"{log_msg} | Beat alternatives: {others}")
    else:
        logger.info(log_msg)

    return winner['id']

def add_read_date_to_hardcover(user_book_id, read_date):
    """Adds a 'Read' entry (date) to the user book."""
    mutation = """
    mutation AddReadDate($user_book_id: Int!, $finished_at: date) {
      insert_user_book_read(user_book_id: $user_book_id, user_book_read: {finished_at: $finished_at}) {
        id
      }
    }
    """
    variables = {
        "user_book_id": user_book_id,
        "finished_at": read_date
    }
    try:
        graphql_query(mutation, variables)
        logger.info(f"  -> Added read date: {read_date}")
        return True
    except Exception as e:
        logger.error(f"  -> Failed to add read date: {e}")
        return False

def add_book_to_hardcover(book_id, rating, read_date=None):
    """
    Adds the book to your 'Read' list.
    Returns: user_book_id (int) if successful, None otherwise.
    """
    mutation = """
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
    """
    variables = {
        "book_id": book_id, 
        "rating": int(rating) if rating else None
    }
    try:
        res = graphql_query(mutation, variables)
        
        # Check for specific error field in return
        if res.get('data', {}).get('insert_user_book'):
            ret_data = res['data']['insert_user_book']
            if ret_data.get('error'):
                logger.error(f"Failed to add book ID {book_id}. API Error: {ret_data['error']}")
                return None
            return ret_data.get('id')
            
        return None
    except Exception as e:
        if "Uniqueness violation" in str(e):
            logger.warning(f"Book ID {book_id} already in library (caught by API error).")
            # Return None because we don't have the user_book_id to add the date
            # (unless we fetched it, but skipping for now as per minimal change)
            return None
        else:
            logger.error(f"Failed to add book ID {book_id}: {e}")
            return None

# --- MAIN LOGIC ---

def sync(dry_run=False, limit=None):
    logger.info(f"--- Starting Sync (Dry Run: {dry_run}, Limit: {limit}) ---")
    
    logger.info(f"Fetching RSS feed...")
    feed = feedparser.parse(RSS_URL)
    if not feed.entries:
        logger.error("No entries found in RSS feed. Check URL.")
        return

    logger.info("Fetching existing Hardcover library...")
    existing_book_ids, existing_isbns_cache, existing_titles_cache = get_hardcover_library_ids()
    logger.info(f"Found {len(existing_book_ids)} existing books in library.")

    # Sort entries:
    # If limit is set, we want the *most recent* 'limit' books.
    if limit:
        logger.info(f"Limiting to the {limit} most recent books from RSS...")
        entries_to_process = feed.entries[:limit]
    else:
        entries_to_process = feed.entries

    # Then reverse to process them chronologically (Oldest -> Newest) relative to that batch
    entries = list(reversed(entries_to_process))
    
    processed_count = 0

    for entry in entries:
        title = entry.title
        author = entry.author_name if 'author_name' in entry else "Unknown"
        rating = entry.user_rating if 'user_rating' in entry else None
        
        # Extract ISBN (try multiple fields)
        isbn = entry.get('isbn13') or entry.get('isbn')
        
        # Extract Date Read
        # Format: "Wed, 14 Jan 2026 00:00:00 +0000" -> "2026-01-14"
        raw_read_at = entry.get('user_read_at')
        if not raw_read_at:
            # Fallback to Date Added if Date Read is missing
            raw_read_at = entry.get('user_date_added')
            if raw_read_at:
                logger.debug(f"Using 'Date Added' as fallback for 'Date Read' for book: {title}")

        read_date = None
        if raw_read_at:
            try:
                dt = parsedate_to_datetime(raw_read_at)
                read_date = dt.strftime('%Y-%m-%d')
            except Exception:
                logger.warning(f"Failed to parse date: {raw_read_at}")
        
        # DEBUG LOGGING
        logger.debug(f"Processing: {title} | ISBN: {isbn} | Auth: {author} | ReadAt: {read_date}")

        # --- OPTIMIZATION: Check Local Cache ---
        # 1. Check ISBN
        if isbn and isbn in existing_isbns_cache:
            logger.info(f"Skipping '{title}': ISBN {isbn} already in library (Local Cache).")
            continue
        
        # 2. Check Exact Title
        clean_title = title.strip().lower()
        if clean_title in existing_titles_cache:
            logger.info(f"Skipping '{title}': Title match already in library (Local Cache).")
            continue
            
        # 3. Check Short Title (Local Cache)
        # Mirroring the API logic: check if cleaned short title is in our local library cache
        match_found_locally = False
        for sep in [':', '(', '-']:
            if sep in title:
                short_title = title.split(sep)[0].strip().lower()
                if len(short_title) < 4: 
                    continue
                
                if short_title in existing_titles_cache:
                    logger.info(f"Skipping '{title}': Short title '{short_title}' matches library (Local Cache).")
                    match_found_locally = True
                    break
        
        if match_found_locally:
            continue
        
        # RATE LIMITING (Only if we proceed to API calls)
        time.sleep(2)

        logger.info(f"New Book Found: '{title}' by {author}")
        
        try:
            hc_book_id = search_hardcover_book_id(title, author, isbn)
            
            if hc_book_id:
                # check duplicate by ID
                if hc_book_id in existing_book_ids:
                    logger.info(f"  -> Book ID {hc_book_id} already in library. Skipping.")
                    continue

                if dry_run:
                    logger.info(f"[DRY RUN] Would add book ID {hc_book_id} (Rating: {rating}, Read: {read_date})")
                else:
                    logger.info(f"-> Adding to library...")
                    user_book_id = add_book_to_hardcover(hc_book_id, rating, read_date)
                    if user_book_id:
                        logger.info(f"-> Success! (UserBookID: {user_book_id})")
                        existing_book_ids.add(hc_book_id)
                        
                        # Add Date Read if available
                        if read_date:
                            add_read_date_to_hardcover(user_book_id, read_date)
                
                processed_count += 1
            else:
                logger.warning(f"-> No match found for '{title}'. Skipping.")
                
        except Exception as e:
            logger.error(f"-> Error processing book: {e}")

    logger.info("--- Sync Complete ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Kindle/Goodreads books to Hardcover")
    parser.add_argument("--dry-run", action="store_true", help="Scan feed but do not send data to Hardcover")
    parser.add_argument("--limit", type=int, help="Maximum number of books to process")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        
    sync(dry_run=args.dry_run, limit=args.limit)