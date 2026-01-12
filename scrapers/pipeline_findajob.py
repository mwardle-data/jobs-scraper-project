from datetime import datetime
import json

from google.cloud import storage
from scrapers.findajob_scraper import (
    get_job_urls,
    scrape_jobs,
    load_seen_ids,
    append_seen_ids,
)

BUCKET_NAME = "scraped-jobs-raw"
URLS_FILE = "urls.jsonl"
JOBS_FILE = "jobs.jsonl"


def upload_file(bucket_name: str, source_file: str, dest_blob: str) -> None:
    # Upload a local file to GCS at the specified path.
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_blob)
    blob.upload_from_filename(source_file)
    print(f"Uploaded {source_file} -> gs://{bucket_name}/{dest_blob}")


def write_new_urls_jsonl(urls, seen_ids, output_file):
    # It saves only new URLs to the JSONL file and returns a list of new URLs together with job_id - url pairs.
    new_urls = []
    new_seen_pairs = []
    now = datetime.utcnow().isoformat()

    with open(output_file, "a", encoding="utf-8") as f:
        for url in urls:
            job_id = url.rstrip("/").split("/")[-1]
            if job_id in seen_ids:
                continue

            rec = {
                "job_id": job_id,
                "url": url,
                "source": "findajob.dwp.gov.uk",
                "first_seen_at": now,
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

            new_urls.append(url)
            new_seen_pairs.append((job_id, url))

    return new_urls, new_seen_pairs


def run_pipeline():
    # Collecting all URLs.
    urls = get_job_urls()
    print(f"Collected {len(urls)} URLs")

    # Loading the know job_ids
    seen_ids = load_seen_ids()
    print(f"Known job_ids: {len(seen_ids)}")

    # Saving only new URLs to urls.jsonl
    new_urls, new_seen_pairs = write_new_urls_jsonl(urls, seen_ids, URLS_FILE)
    print(f"New job ads: {len(new_urls)}")

    # Appending new job_ids to jobs_seen.jsonl
    append_seen_ids(new_seen_pairs)

    # Scraping only new job ads (scrape_jobs saves them to jobs.jsonl)
    scrape_jobs(new_urls)

    # Uploading files to Google Cloud Storage
    today = datetime.utcnow().strftime("%Y-%m-%d")
    upload_file(BUCKET_NAME, URLS_FILE, f"urls/{today}/urls.jsonl")
    upload_file(BUCKET_NAME, JOBS_FILE, f"jobs/{today}/jobs.jsonl")


if __name__ == "__main__":
    run_pipeline()
