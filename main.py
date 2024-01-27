from requests import Session
import re, sqlite3, time, os
from queue import Queue
WAIT_INTERVAL_IN_SECONDS = 1
words = ["Madeira", "Funchal"] # List of words to be searched
INITIAL_CATEGORIES = ["Category:Files needing categories"] # List of categories to be searched





CACHE_NAME = "cache.db"
EXPORT_NAME = "uncategorized_files.wiki.txt"
word_pattern = re.compile("(" + "|".join(words) + ")") # Compile regex pattern
SQL_INIT = """
CREATE TABLE IF NOT EXISTS `files` (
    `title`	TEXT PRIMARY KEY,
    `where_matched`	TEXT,
    `what_matched`	TEXT,
    `category`	TEXT
);"""
SQL_INSERT = """
INSERT OR IGNORE INTO `files` (`title`, `where_matched`, `what_matched`, `category`) VALUES (?, ?, ?, ?);"""
SQL_SELECT = """
SELECT * FROM `files`"""
URL = "https://commons.wikimedia.org/w/api.php"

categories_to_be_searched = Queue() # Queue of categories to be searched



def go_thorugh_category(cat : str, queue : Queue,  session : Session):
    print("Going through", cat)
    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "generator": "categorymembers",
        "formatversion": "2",
        "rvprop": "content",
        "rvslots": "main",
        "gcmtitle": cat
    }
    while True:
        print(f"Waiting {WAIT_INTERVAL_IN_SECONDS} seconds")
        time.sleep(WAIT_INTERVAL_IN_SECONDS)
        r = session.get(URL, params=params)
        data = r.json()
        if "error" in data:
            print(data)
            break
        print("Got data")
        if "query" not in data:
            print(data)
            break
        for page in data["query"]["pages"]:
            if page['ns'] == 14:
                print("Enqueuing category: " + page["title"])
                queue.put(page["title"])
                continue
            found_words = word_pattern.search(page["title"])
            if found_words is not None:
                print("Found word in title: " + page["title"])
                yield page["title"], "Title", found_words.group(0), cat
                continue

            # Search for words in content
            if "revisions" not in page:
                continue
            rev = page["revisions"][0]
            if "content" not in rev["slots"]["main"]:
                continue
            content = rev["slots"]["main"]["content"]
            found_words = word_pattern.search(content)
            if found_words is not None:
                print("Found word in content: " + page["title"])
                yield page["title"], "Content", found_words.group(0), cat
                continue
        if "continue" not in data:
            break
        params.update(data["continue"])

def search_iteratively(q : Queue, session : Session):
    while not q.empty():
        cat = q.get()
        with sqlite3.connect(CACHE_NAME) as db:
            db.executemany(SQL_INSERT, go_thorugh_category(cat, q, session))
            print("Commiting")
            db.commit()
        break

def main():
    with sqlite3.connect(CACHE_NAME) as db:
        db.executescript(SQL_INIT)
        db.commit()
    session = Session()
    for cat in INITIAL_CATEGORIES:
        categories_to_be_searched.put(cat)
    search_iteratively(categories_to_be_searched, session)
    db.close()
    
def export():
    db = sqlite3.connect(CACHE_NAME)
    db.row_factory = sqlite3.Row
    with db:
        with open(EXPORT_NAME , "w") as f:
            f.write("""
{| class="wikitable sortable" style="text-align: center;" 
! File
! Where Matched
! What Matched
! Category
! Thumbnail
            """)
            for row in db.execute(SQL_SELECT):
                f.write("|-\n")
                f.write("| [[:" + row["title"] + "]]\n")
                f.write("| " + row["where_matched"] + "\n")
                f.write("| " + row["what_matched"] + "\n")
                f.write("| " + row["category"] + "\n")
                f.write("| [[File:" + row["title"] + "|thumb|100px]]\n")
            f.write("|}")
    db.close()
if __name__ == "__main__":
    main()
    export()
    os.remove(CACHE_NAME)