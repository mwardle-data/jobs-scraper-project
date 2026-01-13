import requests
import time
import json
import os
from bs4 import BeautifulSoup
from datetime import datetime


SEEN_FILE = "jobs_seen.jsonl"


def load_seen_ids(path: str = SEEN_FILE) -> set:
    seen = set()
    if not os.path.exists(path):
        return seen
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                jid = rec.get("job_id")
                if jid:
                    seen.add(jid)
            except json.JSONDecodeError:
                continue
    return seen


def append_seen_ids(job_ids, path: str = SEEN_FILE) -> None:
    now = datetime.utcnow().isoformat()
    with open(path, "a", encoding="utf-8") as f:
        for jid, url in job_ids:
            rec = {
                "job_id": jid,
                "url": url,
                "first_seen_at": now
            }
            f.write(json.dumps(rec) + "\n")


def get_job_urls():
    base_url = "https://findajob.dwp.gov.uk/search"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:145.0) Gecko/20100101 Firefox/145.0"
    }

# Params that stay the same across pages
    search_params = {
        "q": "data+engineer",
        "w": "UK",
        "pp": "50",
        "p": None
    }

    all_ads_links = []
    page_no = 1

    while True:
        # Updating only the page number.
        search_params["p"] = page_no

        response = requests.get(base_url, params=search_params, headers=headers)
        response.raise_for_status()

        soup_all = BeautifulSoup(response.text, "html.parser")

        # get only job detail links
        ads_links = [
            a["href"]
            for a in soup_all.select("a.govuk-link[href*='/details/']")
        ]

        if not ads_links:
            break

        all_ads_links.extend(ads_links)
        page_no += 1
        time.sleep(1)

    return all_ads_links


def scrape_jobs(urls_to_scrape):
    output_file = "jobs.jsonl"
    f = open(output_file, "a", encoding="utf-8")

    for url in urls_to_scrape:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # will error if e.g. 403/404

        soup = BeautifulSoup(response.text, "html.parser")

        job_data = {}

        job_id = url.rstrip("/").split("/")[-1]
        job_data["job_id"] = job_id

        # Adding a few extra fields from elsewhere on the page (e.g. title)
        title_tag = soup.find("h1")
        if title_tag:
            job_data["Job title"] = title_tag.get_text(strip=True)

        # Finding the job details table
        job_table = soup.find("table")  # or soup.find("table", class_="job-details")

        # Turning table rows into a dict
        for row in job_table.find_all("tr"):
            header_cell = row.find("th")
            value_cell = row.find("td")
            if not header_cell or not value_cell:
                continue
            key = header_cell.get_text(strip=True).split(":", 1)[0]
            value = value_cell.get_text(separator=" ", strip=True)
            job_data[key] = value

        summary_tag = soup.find("h2")
        if summary_tag:
            job_data["Summary"] = soup.find(
                'div',
                class_="govuk-body govuk-!-margin-bottom-6",
                itemprop="description"
            ).text.strip().replace('\n', ';')

        #  Print as JSON
        f.write(json.dumps(job_data, ensure_ascii=False) + "\n")

    f.close()


if __name__ == "__main__":
    # Collecting URLs as before
    urls = get_job_urls()
    print("Number of collected URLs:", len(urls))

    # Loading already seen job ads
    seen_ids = load_seen_ids()
    print("Already seen job_id:", len(seen_ids))

    # 3. Extracting job_ids from URLs and filtering out only new ones
    new_urls = []
    new_seen_pairs = []  # (job_id, url)
    for job_url in urls:
        job_id_from_url = job_url.rstrip("/").split("/")[-1]
        if job_id_from_url in seen_ids:
            continue

        new_urls.append(job_url)
        new_seen_pairs.append((job_id_from_url, job_url))

    print("Number of new ads to be scraped:", len(new_urls))

    # 4. Appending new job_ids to jobs_seen.jsonl
    append_seen_ids(new_seen_pairs)

    # 5. Scraping only new job ads
    scrape_jobs(new_urls)
