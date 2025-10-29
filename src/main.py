import argparse
import os
import re
import time
import logging
from typing import Any
import dns.resolver

import gspread
import requests
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from pydantic import BaseModel, Field


class Config(BaseModel):
    apify_token: str
    google_sheet_id: str
    google_sheet_name: str = "scraped_leads"
    search_term: str = "plumber"
    location: str = "Manchester, GB"
    max_results: int = 120
    poll_interval: int = 30


class ApifyScrapeRequest(BaseModel):
    searchStringsArray: list[str]
    locationQuery: str
    maxCrawledPlacesPerSearch: int
    language: str = "en"
    maximumLeadsEnrichmentRecords: int = 0
    maxImages: int = 0


class ApifyJobData(BaseModel):
    id: str
    status: str
    defaultDatasetId: str | None = None


class ScrapedLead(BaseModel):
    title: str | None = None
    phone: str | None = None
    address: str | None = None
    website: str | None = None
    email: str | None = None
    instagram: str | None = None
    facebook: str | None = None
    linkedin: str | None = None


class ExistingLead(BaseModel):
    Business: str | None = Field(default=None, alias="Business")
    Phone: str | None = Field(default=None, alias="Phone")

    class Config:
        populate_by_name = True


class ApifyService:
    def __init__(self, token: str) -> None:
        self.token = token
        self.base_url = "https://api.apify.com/v2"
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def start_scrape(self, request: ApifyScrapeRequest) -> str:
        url = f"{self.base_url}/acts/compass~crawler-google-places/runs"
        response = requests.post(
            url,
            json=request.model_dump(),
            headers=self.headers,
            params={"token": self.token},
            timeout=30,
        )
        response.raise_for_status()
        job_id = response.json()["data"]["id"]
        logging.info(f"Started Apify job with ID: {job_id}")
        return job_id

    def get_job_status(self, job_id: str) -> ApifyJobData:
        url = f"{self.base_url}/actor-runs/{job_id}"
        response = requests.get(
            url, headers=self.headers, params={"token": self.token}, timeout=10
        )
        response.raise_for_status()
        data = response.json()["data"]
        logging.debug(f"Job {job_id} status: {data['status']}")
        return ApifyJobData(**data)

    def get_dataset(self, dataset_id: str) -> list[dict[str, Any]]:
        logging.info(f"Fetching dataset {dataset_id}")
        url = f"{self.base_url}/datasets/{dataset_id}/items"
        response = requests.get(
            url, headers=self.headers, params={"token": self.token}, timeout=10
        )
        response.raise_for_status()
        return response.json()


class LinkVerificationService:
    """Service to verify Instagram and Facebook links."""

    def __init__(self) -> None:
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }

    def verify_instagram(self, url: str) -> bool:
        """Verify if an Instagram link is valid."""
        if not url or not url.strip():
            return False

        try:
            logging.info(f"Verifying Instagram link: {url}")
            response = requests.head(
                url, headers=self.headers, timeout=10, allow_redirects=True
            )

            # Check if it's a valid response (not 404, 403, etc.)
            if response.status_code >= 400:
                logging.warning(
                    f"Instagram link returned {response.status_code}: {url}"
                )
                return False

            # Check if it redirected to login or homepage (common for invalid profiles)
            final_url = response.url.lower()
            if "accounts/login" in final_url or final_url.endswith("instagram.com/"):
                logging.warning(f"Instagram link redirected to login/homepage: {url}")
                return False

            logging.info(f"Instagram link is valid: {url}")
            return True

        except requests.exceptions.Timeout:
            logging.warning(f"Instagram link timed out: {url}")
            return False
        except requests.exceptions.RequestException as e:
            logging.warning(f"Error verifying Instagram link {url}: {e}")
            return False

    def verify_facebook(self, url: str) -> bool:
        """Verify if a Facebook link is valid."""
        if not url or not url.strip():
            return False

        try:
            logging.info(f"Verifying Facebook link: {url}")
            response = requests.head(
                url, headers=self.headers, timeout=10, allow_redirects=True
            )

            # Check if it's a valid response
            if response.status_code >= 400:
                logging.warning(f"Facebook link returned {response.status_code}: {url}")
                return False

            # Check if it redirected to login or homepage
            final_url = response.url.lower()
            if "login" in final_url or final_url.endswith("facebook.com/"):
                logging.warning(f"Facebook link redirected to login/homepage: {url}")
                return False

            logging.info(f"Facebook link is valid: {url}")
            return True

        except requests.exceptions.Timeout:
            logging.warning(f"Facebook link timed out: {url}")
            return False
        except requests.exceptions.RequestException as e:
            logging.warning(f"Error verifying Facebook link {url}: {e}")
            return False


class EmailValidationService:
    """Service to validate email addresses."""

    def __init__(self) -> None:
        self.email_pattern = re.compile(
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Z|a-z]{2,}$"
        )

    def validate_format(self, email: str) -> bool:
        """Check if email has valid format."""
        if not email or not email.strip():
            return False
        return bool(self.email_pattern.match(email.strip()))

    def validate_domain(self, email: str) -> bool:
        """Check if email domain has valid MX records."""
        if not self.validate_format(email):
            return False

        try:
            domain = email.split("@")[1]
            # Check if domain has MX records
            mx_records = dns.resolver.resolve(domain, "MX")
            return len(list(mx_records)) > 0
        except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer, dns.resolver.Timeout):
            logging.debug(f"No MX records found for domain: {domain}")
            return False
        except Exception as e:
            logging.debug(f"Error validating domain for {email}: {e}")
            return False

    def validate_email(self, email: str, check_domain: bool = True) -> bool:
        """Validate email format and optionally check domain."""
        if not self.validate_format(email):
            return False
        if check_domain:
            return self.validate_domain(email)
        return True


class WebsiteScraperService:
    def scrape_website(self, url: str) -> dict[str, str | None]:
        """Scrape a website and extract email and social media links using regex."""
        result: dict[str, str | None] = {
            "email": None,
            "instagram": None,
            "facebook": None,
            "linkedin": None,
        }

        try:
            logging.info(f"Scraping website: {url}")

            # Add http:// if no scheme is present
            if not url.startswith(("http://", "https://")):
                url = "https://" + url

            # Fetch website content
            response = requests.get(
                url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10
            )
            response.raise_for_status()
            content = response.text

            # Extract email using regex
            email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
            emails = re.findall(email_pattern, content)
            if emails:
                result["email"] = emails[0]
                logging.info(f"Found email: {emails[0]}")

            # Extract social media links
            instagram_pattern = (
                r"(?:https?://)?(?:www\.)?instagram\.com/([a-zA-Z0-9._]+)"
            )
            facebook_pattern = r"(?:https?://)?(?:www\.)?facebook\.com/([a-zA-Z0-9._]+)"
            linkedin_pattern = r"(?:https?://)?(?:www\.)?linkedin\.com/(?:company|in)/([a-zA-Z0-9._-]+)"

            instagram_matches = re.findall(instagram_pattern, content)
            if instagram_matches:
                result["instagram"] = f"https://instagram.com/{instagram_matches[0]}"
                logging.info(f"Found Instagram: {result['instagram']}")

            facebook_matches = re.findall(facebook_pattern, content)
            if facebook_matches:
                result["facebook"] = f"https://facebook.com/{facebook_matches[0]}"
                logging.info(f"Found Facebook: {result['facebook']}")

            linkedin_matches = re.findall(linkedin_pattern, content)
            if linkedin_matches:
                result["linkedin"] = (
                    f"https://linkedin.com/company/{linkedin_matches[0]}"
                )
                logging.info(f"Found LinkedIn: {result['linkedin']}")

        except Exception as e:
            logging.error(f"Error scraping {url}: {e}")

        return result


class GoogleSheetsService:
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    def __init__(
        self, sheet_id: str, sheet_name: str, creds_file: str = "credentials.json"
    ) -> None:
        self.sheet_id = sheet_id
        self.sheet_name = sheet_name
        self.creds_file = creds_file
        self.worksheet = self._get_worksheet()

    def _get_worksheet(self) -> gspread.Worksheet:
        creds = Credentials.from_service_account_file(
            self.creds_file, scopes=self.SCOPES
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(self.sheet_id)

        # Try to get the worksheet by name, or create it if it doesn't exist
        try:
            return spreadsheet.worksheet(self.sheet_name)
        except gspread.WorksheetNotFound:
            logging.info(f"Worksheet '{self.sheet_name}' not found. Creating it.")
            return spreadsheet.add_worksheet(title=self.sheet_name, rows=100, cols=20)

    def read_leads(self) -> list[ExistingLead]:
        logging.info("Reading existing leads from Google Sheet.")
        all_values = self.worksheet.get_all_values()

        if not all_values:
            logging.info("No existing leads found.")
            return []

        headers = all_values[0]
        leads = []
        for row in all_values[1:]:
            lead_dict = {
                headers[i]: row[i] if i < len(row) else None
                for i in range(len(headers))
            }
            leads.append(ExistingLead(**lead_dict))

        logging.info(f"Found {len(leads)} existing leads.")
        return leads

    def append_leads(self, leads: list[dict[str, str]]) -> None:
        if not leads:
            logging.info("No new leads to append.")
            return

        all_values = self.worksheet.get_all_values()
        headers: list[str]
        if not all_values or not all_values[0]:
            logging.info("No headers found. Adding headers.")
            headers = [
                "Name",
                "Phone",
                "Address",
                "Website",
                "Email",
                "Instagram",
                "Facebook",
                "LinkedIn",
            ]
            self.worksheet.append_row(headers)
        else:
            headers = all_values[0]

        logging.info(f"Appending {len(leads)} new leads to the sheet.")

        scraped_to_sheet_map = {
            "title": "Name",
            "phone": "Phone",
            "address": "Address",
            "website": "Website",
            "email": "Email",
            "instagram": "Instagram",
            "facebook": "Facebook",
            "linkedin": "LinkedIn",
        }

        values_to_append = []
        for lead in leads:
            row_dict = {
                sheet_header: lead.get(scraped_key, "")
                for scraped_key, sheet_header in scraped_to_sheet_map.items()
            }
            row = [row_dict.get(h, "") for h in headers]
            values_to_append.append(row)

        if values_to_append:
            self.worksheet.append_rows(
                values_to_append, value_input_option="USER_ENTERED"
            )

    def remove_duplicates(self) -> None:
        """Remove duplicate rows based on the first column (Name)."""
        logging.info("Reading all rows from the sheet...")
        all_values = self.worksheet.get_all_values()

        if not all_values or len(all_values) <= 1:
            logging.info("Sheet is empty or only has headers. Nothing to dedupe.")
            return

        headers = all_values[0]
        data_rows = all_values[1:]
        original_count = len(data_rows)

        logging.info(f"Found {original_count} data rows (excluding header).")

        # Track seen values in the first column
        seen_names: set[str] = set()
        unique_rows = [headers]  # Start with headers

        duplicates_removed = 0
        for row in data_rows:
            # Get the first column value (Name)
            if not row:  # Skip completely empty rows
                continue

            first_col_value = row[0].strip().lower() if row[0] else ""

            # Keep row if first column is empty or hasn't been seen before
            if not first_col_value:
                logging.debug("Found row with empty first column, keeping it.")
                unique_rows.append(row)
            elif first_col_value not in seen_names:
                seen_names.add(first_col_value)
                unique_rows.append(row)
            else:
                duplicates_removed += 1
                logging.debug(f"Removing duplicate: {row[0]}")

        if duplicates_removed == 0:
            logging.info("No duplicates found!")
            return

        logging.info(f"Removing {duplicates_removed} duplicate rows...")

        # Clear the sheet and write back the unique rows
        self.worksheet.clear()
        self.worksheet.update(unique_rows, value_input_option="USER_ENTERED")

        logging.info(
            f"Deduplication complete! Removed {duplicates_removed} duplicates. "
            f"Kept {len(unique_rows) - 1} unique rows."
        )

    def get_all_emails(self) -> list[str]:
        """Extract all emails from the Email column."""
        logging.info("Reading all emails from the sheet...")
        all_values = self.worksheet.get_all_values()

        if not all_values or len(all_values) <= 1:
            logging.info("Sheet is empty or only has headers.")
            return []

        headers = all_values[0]
        data_rows = all_values[1:]

        # Find Email column index
        email_col = None
        for i, header in enumerate(headers):
            if header.lower() == "email":
                email_col = i
                break

        if email_col is None:
            logging.warning("No Email column found in the sheet.")
            return []

        # Extract all non-empty emails
        emails = []
        for row in data_rows:
            if email_col < len(row):
                email = row[email_col].strip()
                if email:
                    emails.append(email)

        logging.info(f"Found {len(emails)} emails in the sheet.")
        return emails

    def verify_and_clean_links(self, verifier: "LinkVerificationService") -> None:
        """Verify Instagram and Facebook links and clear invalid ones."""
        logging.info("Reading all rows from the sheet for verification...")
        all_values = self.worksheet.get_all_values()

        if not all_values or len(all_values) <= 1:
            logging.info("Sheet is empty or only has headers. Nothing to verify.")
            return

        headers = all_values[0]
        data_rows = all_values[1:]

        # Find column indices for Instagram and Facebook
        instagram_col = None
        facebook_col = None
        for i, header in enumerate(headers):
            if header.lower() == "instagram":
                instagram_col = i
            elif header.lower() == "facebook":
                facebook_col = i

        if instagram_col is None and facebook_col is None:
            logging.warning("No Instagram or Facebook columns found in the sheet.")
            return

        logging.info(f"Found {len(data_rows)} rows to verify.")

        instagram_cleaned = 0
        facebook_cleaned = 0
        updated_rows = [headers]

        for row_idx, row in enumerate(data_rows, start=1):
            # Ensure row has enough columns
            while len(row) < len(headers):
                row.append("")

            row_modified = False

            # Verify Instagram if present
            if instagram_col is not None and instagram_col < len(row):
                instagram_url = row[instagram_col].strip()
                if instagram_url and not verifier.verify_instagram(instagram_url):
                    logging.info(
                        f"Row {row_idx}: Clearing invalid Instagram link: {instagram_url}"
                    )
                    row[instagram_col] = ""
                    instagram_cleaned += 1
                    row_modified = True

            # Verify Facebook if present
            if facebook_col is not None and facebook_col < len(row):
                facebook_url = row[facebook_col].strip()
                if facebook_url and not verifier.verify_facebook(facebook_url):
                    logging.info(
                        f"Row {row_idx}: Clearing invalid Facebook link: {facebook_url}"
                    )
                    row[facebook_col] = ""
                    facebook_cleaned += 1
                    row_modified = True

            updated_rows.append(row)

        total_cleaned = instagram_cleaned + facebook_cleaned
        if total_cleaned == 0:
            logging.info("All links are valid! No changes needed.")
            return

        logging.info(f"Updating sheet with cleaned links...")
        logging.info(f"Instagram links cleaned: {instagram_cleaned}")
        logging.info(f"Facebook links cleaned: {facebook_cleaned}")

        # Update the entire sheet with cleaned data
        self.worksheet.clear()
        self.worksheet.update(updated_rows, value_input_option="USER_ENTERED")

        logging.info(f"Verification complete! Cleaned {total_cleaned} invalid links.")


class LeadScraperWorkflow:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.apify = ApifyService(config.apify_token)
        self.scraper = WebsiteScraperService()
        self.sheets = GoogleSheetsService(
            config.google_sheet_id, config.google_sheet_name
        )

    def _clean_phone(self, phone: str) -> str:
        return phone.replace("+", "").replace(" ", "")

    def _filter_new_leads(
        self, scraped: list[dict[str, Any]], existing: list[ExistingLead]
    ) -> list[dict[str, str]]:
        existing_phones = {
            self._clean_phone(lead.Phone) for lead in existing if lead.Phone is not None
        }

        # Track existing names for deduplication
        existing_names = {
            lead.Name.strip().lower()
            for lead in existing
            if lead.Name is not None and lead.Name.strip()
        }

        new_leads = []
        for item in scraped:
            phone = item.get("phone")
            title = item.get("title", "").strip()

            if phone and title:
                clean_phone = self._clean_phone(phone)
                normalized_name = title.lower()

                # Check if both phone and name are unique
                if (
                    clean_phone not in existing_phones
                    and normalized_name not in existing_names
                ):
                    lead_data = {
                        "title": title,
                        "phone": clean_phone,
                        "address": item.get("address", ""),
                        "website": item.get("website", ""),
                        "email": "",
                        "instagram": "",
                        "facebook": "",
                        "linkedin": "",
                    }
                    new_leads.append(lead_data)
                    # Add to tracking sets to prevent duplicates within this batch
                    existing_phones.add(clean_phone)
                    existing_names.add(normalized_name)
                elif normalized_name in existing_names:
                    logging.debug(f"Skipping duplicate name: {title}")
                elif clean_phone in existing_phones:
                    logging.debug(f"Skipping duplicate phone: {clean_phone}")

        logging.info(
            f"Found {len(scraped)} scraped leads, {len(new_leads)} of them are new."
        )
        return new_leads

    def _enrich_leads(self, leads: list[dict[str, str]]) -> list[dict[str, str]]:
        """Enrich leads by scraping their websites for email and social media links."""
        enriched_leads = []
        for lead in leads:
            website = lead.get("website", "")
            if website:
                scraped_data = self.scraper.scrape_website(website)
                lead["email"] = scraped_data.get("email") or ""
                lead["instagram"] = scraped_data.get("instagram") or ""
                lead["facebook"] = scraped_data.get("facebook") or ""
                lead["linkedin"] = scraped_data.get("linkedin") or ""
            enriched_leads.append(lead)

        return enriched_leads

    def _wait_for_completion(self, job_id: str) -> str:
        logging.info(f"Waiting for job {job_id} to complete...")
        while True:
            status = self.apify.get_job_status(job_id)
            if status.status == "SUCCEEDED":
                logging.info(f"Job {job_id} succeeded.")
                if status.defaultDatasetId is None:
                    msg = "Job succeeded but no dataset ID found"
                    logging.error(msg)
                    raise ValueError(msg)
                return status.defaultDatasetId
            logging.info(f"Job {job_id} status is {status.status}. Waiting...")
            time.sleep(self.config.poll_interval)

    def run(self) -> None:
        logging.info("Starting lead scraper workflow.")
        request = ApifyScrapeRequest(
            searchStringsArray=[self.config.search_term],
            locationQuery=self.config.location,
            maxCrawledPlacesPerSearch=self.config.max_results,
        )

        job_id = self.apify.start_scrape(request)
        dataset_id = self._wait_for_completion(job_id)
        scraped_data = self.apify.get_dataset(dataset_id)
        existing_leads = self.sheets.read_leads()
        new_leads = self._filter_new_leads(scraped_data, existing_leads)

        # Enrich leads with website data
        logging.info(f"Enriching {len(new_leads)} leads with website data...")
        enriched_leads = self._enrich_leads(new_leads)

        self.sheets.append_leads(enriched_leads)

        logging.info(f"Added {len(enriched_leads)} new leads to sheet")


def main() -> None:
    load_dotenv()

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Lead scraper tool")
    parser.add_argument(
        "--dedupe",
        action="store_true",
        help="Remove duplicate rows from the sheet based on the first column (Name). No scraping will be performed.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify Instagram and Facebook links and remove invalid ones from the sheet. No scraping will be performed.",
    )
    parser.add_argument(
        "--emails",
        action="store_true",
        help="Extract and display all unique validated emails from the sheet. No scraping will be performed.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.info("Starting application.")

    config = Config(
        apify_token=os.getenv("APIFY_TOKEN", ""),
        google_sheet_id=os.getenv("GOOGLE_SHEET_ID", ""),
        google_sheet_name=os.getenv("GOOGLE_SHEET_NAME", "scraped_leads"),
        search_term=os.getenv("SEARCH_TERM", "plumber"),
        location=os.getenv("LOCATION", "Manchester, GB"),
        max_results=int(os.getenv("MAX_RESULTS", "120")),
        poll_interval=int(os.getenv("POLL_INTERVAL", "30")),
    )
    logging.info("Configuration loaded.")

    # Dedupe mode - just remove duplicates and exit
    if args.dedupe:
        logging.info("Running in dedupe mode...")
        sheets = GoogleSheetsService(config.google_sheet_id, config.google_sheet_name)
        sheets.remove_duplicates()
        logging.info("Dedupe finished.")
        return

    # Verify mode - verify and clean invalid social media links
    if args.verify:
        logging.info("Running in verify mode...")
        sheets = GoogleSheetsService(config.google_sheet_id, config.google_sheet_name)
        verifier = LinkVerificationService()
        sheets.verify_and_clean_links(verifier)
        logging.info("Verification finished.")
        return

    # Emails mode - extract and validate emails
    if args.emails:
        logging.info("Running in emails mode...")
        sheets = GoogleSheetsService(config.google_sheet_id, config.google_sheet_name)
        email_validator = EmailValidationService()

        # Get all emails from sheet
        all_emails = sheets.get_all_emails()

        # Get unique emails
        unique_emails = list(set(all_emails))
        logging.info(
            f"Found {len(unique_emails)} unique emails out of {len(all_emails)} total."
        )

        # Validate emails
        valid_emails = []
        invalid_emails = []

        logging.info("Validating emails...")
        for email in unique_emails:
            if email_validator.validate_email(email, check_domain=True):
                valid_emails.append(email)
            else:
                invalid_emails.append(email)
                logging.debug(f"Invalid email: {email}")

        # Print results
        print("\n" + "=" * 60)
        print(f"VALIDATED UNIQUE EMAILS ({len(valid_emails)} total)")
        print("=" * 60)
        for email in sorted(valid_emails):
            print(email)

        if invalid_emails:
            print("\n" + "=" * 60)
            print(f"INVALID EMAILS ({len(invalid_emails)} total)")
            print("=" * 60)
            for email in sorted(invalid_emails):
                print(email)

        print("\n" + "=" * 60)
        logging.info(
            f"Emails mode finished. Valid: {len(valid_emails)}, Invalid: {len(invalid_emails)}"
        )
        return

    # Normal scraping mode
    workflow = LeadScraperWorkflow(config)
    workflow.run()
    logging.info("Workflow finished.")


if __name__ == "__main__":
    main()
