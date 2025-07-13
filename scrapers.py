import requests
import re
import datetime
import patoolib
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pathlib import Path


class BaseScraper(ABC):
    """
        Abstract scraper class which serves as a base for all other scrapers.
    """

    @abstractmethod
    def __init__(self, config: dict, scraper_config: dict):
        """
            BaseScraper constructor.

        Args:
            config (dict): Script configuration
            scraper_config (dict): Scraper configuration
        """
        pass

    @abstractmethod
    def scrape_files():
        """
            Scrape files from specified web pages and save them locally 
            for processing later.
        """
        pass


class ScraperPCR(BaseScraper):
    """
        Scraper which scrapes traffic accident data from web pages of Policie ČR.
    """

    def __init__(self, config: dict, scraper_config: dict):
        """
            ScraperPCR constructor.

        Args:
            config (dict): Script configuration
            scraper_config (dict): Scraper configuration
        """

        self.config = config
        self.scraper_config = scraper_config

    def get_latest_entry_year(self, parsed: BeautifulSoup) -> str:
        """
            Get last available year.

        Args:
            parsed (BeautifulSoup): Parsed page with summary of available years

        Returns:
            str: Last year present
        """

        latest_year_tag = parsed.select_one("ol li span")
        if latest_year_tag == None:
            return None

        latest_year = latest_year_tag.find(string=re.compile("^[0-9]{4}$"))
        if latest_year == None:
            return None

        return latest_year.string

    def get_links(self) -> tuple[dict | None]:
        """
            Get dictionary of years which are available, starting from 2016.

        Returns:
            tuple[dict | None]: Dictionary of available years or None when not found
        """

        response = requests.get(self.scraper_config["target_url"])
        if response.status_code >= 400:
            return None
        parsed = BeautifulSoup(response.content, self.scraper_config["parser"])

        latest_entry_year = self.get_latest_entry_year(parsed)
        if latest_entry_year != None:

            # Get dictionary of available years
            available_years = parsed.select("ol li a")
            year_links = {latest_entry_year: self.scraper_config["target_url"]}
            if len(available_years) > 0:
                for item in available_years:
                    # Ignore years which contain only pdf files
                    if int(item.string) < 2016:
                        continue

                    link = item["href"]
                    if not item["href"].startswith("/"):
                        link = "/" + link

                    year_links[item.string] = urljoin(
                        self.scraper_config["target_url"], link)
            return year_links
        return None

    def download_archive(self, page: BeautifulSoup) -> tuple[Path | None, Path | None]:
        """
            Download archives from specific year page. Lastest archive should be downloaded
            as it includes all of the data of previous ones as well as the new data.

        Args:
            page (BeautifulSoup): Parsed page for specific year

        Returns:
            tuple[Path | None, Path | None]: Path to downloaded archive or None,
            when no download links were found.
        """

        previous_years = "[^0-9]*[0-9]{4}[^0-9]*"
        download_links = page.find_all(
            "a", href=re.compile(f"^soubor/.*data({previous_years}|.*{datetime.datetime.now().year}.*)(zip|rar).aspx$"))

        if len(download_links) == 0:
            return None, None

        link = download_links[0]
        title = str(download_links[0]["title"]).split(".")[0]
        folder_path = Path(self.scraper_config["extract_folder"] + title + "/")

        if folder_path.exists():
            return None, None
        folder_path.mkdir(parents=True, exist_ok=True)

        archive_path: Path = folder_path / link["title"]
        if not archive_path.exists():
            href = link["href"]
            if not link["href"].startswith("/"):
                href = "/" + href

            resp = requests.get(
                urljoin(self.scraper_config["target_url"], href))
            with open(archive_path, "wb") as zipFile:
                zipFile.write(resp.content)
        return archive_path, folder_path

    def scrape_files(self):
        """
            Scrape traffic accident files from Policie ČR web pages.
        """

        year_links = self.get_links()
        for year, link in year_links.items():

            # Scrape and download archives
            page = requests.get(link, timeout=10)
            parsed = BeautifulSoup(page.content, self.scraper_config["parser"])
            archive_path, folder_path = self.download_archive(parsed)

            if archive_path != None and archive_path.exists():
                patoolib.extract_archive(
                    archive_path, outdir=folder_path, verbosity=-1)

                # Remove unused files
                for archive_file in folder_path.iterdir():
                    if archive_file.name.split(".")[0] not in self.config["data_files"]:
                        archive_file.unlink()
