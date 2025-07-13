import json
import logging
import extractors
import scrapers
import loaders
import api_loaders
import api_extractors
import os
import datetime
import pandas as pd
from pathlib import Path
from jsonschema import validate


def get_log_level(level: str) -> int:
    """
        Map level selected in the config to logger level.

    Args:
        level (str): Level set in configuration file

    Returns:
        int: Logger level
    """
    level = level.lower()
    if level == "critical":
        return 50  # logging.CRITICAL
    elif level == "error":
        return 40  # logging.ERROR
    elif level == "warning":
        return 30  # logging.WARNING
    elif level == "debug":
        return 10  # logging.DEBUG
    else:
        return 20  # logging.INFO


def load_config() -> dict:
    """Load contents of the configuration file.

    Returns:
        dict: Dictionary containing script configuration
    """
    with open("config.json", "r") as config_file:
        config = config_file.read()
        config = json.loads(config)
        with open("schema.json", "r") as schema_file:
            schema = schema_file.read()
            schema = json.loads(schema)
            validate(instance=config, schema=schema)
            return config


def sort_files(filename: str, config: dict) -> int:
    """
        Sort processed files according to their priority
        and presence of spatial data.

    Args:
        filename (str): Filename
        config (dict): Script configuration

    Returns:
        int: Priority of given file
    """
    if filename in config["data_files"]:
        file_config = config["data_files"][filename]
        if file_config["coordinates"] != None:
            return 0

    # Sort files to be processed in the specified order
    return max(1, config["data_files"][filename]["file_order"])


def process_data(config: dict):
    """
        Get, process and store traffic data. Multiple options for
        scraping, processing and storing are available. Selected
        options and are injected into relevant parts of the code.
        Most things can be modified within the configuration file.
        In situations where there is a need for larger changes there
        is an option to implement new options which extend relevant
        base classes. These new option can then be injected the same
        way as previous ones as the modules are plug in and no other
        changes should be necessary.

    Args:
        config (dict): Script configuration
    """

    # Run scrapers specified in the config file
    for scraper_config in config["scrapers"]:
        try:
            # Select scraper from scrapers.py
            scraper = getattr(
                scrapers, scraper_config["scraper"])(config, scraper_config)

            logger.info(f"Scraping {scraper_config['target_url']}")
            scraper.scrape_files()
            logger.debug(f"Scraped {scraper_config['target_url']}")
        except BaseException as e:
            logger.warning(
                f"Exception {e=}, {type(e)=} occured, during scraping of {scraper_config['target_url']}")

    for api_config in config["apis"]:
        try:
            logger.info(f"Processing API: {api_config['url']}")
            if "api_extractor" not in api_config or api_config["api_extractor"] == None:
                logger.warning(
                    f"Skipped {api_config['url']}, no extractor specified.")
                continue

            if "api_loader" not in api_config or api_config["api_loader"] == None:
                logger.warning(
                    f"Skipped {api_config['url']}, no loader specified.")
                continue

            # Select data api_extractor from api_extractors.py
            api_extractor = getattr(
                api_extractors, api_config["api_extractor"])(config, api_config)
            logger.debug(f"Selected extractor {api_extractor}")

            logger.info(f"Extracting {api_config['url']}")
            data = api_extractor.extract_data()
            logger.debug(f"Extracted {api_config['url']}")

            # Select data api_loader from api_loaders.py
            api_loader = getattr(api_loaders, api_config["api_loader"])(
                config, api_config)
            logger.debug(f"Selected loader {api_loader}")

            logger.info(f"Loading {len(data)} new entries")
            api_loader.store_data(data)
            logger.debug(f"Loaded entries")
        except BaseException as e:
            logger.warning(
                f"Exception {e=}, {type(e)=} occured processing API {api_config['url']}")

    logger.debug("Processed APIs")

    # Get number of directories
    dir_count = len(next(os.walk(config["data_folder"]))[1])

    # Iterate over data files
    for iteration, [dirpath, _, filenames] in enumerate(os.walk(config["data_folder"])):
        if len(filenames) == 0:
            logger.debug(f"No files found at {dirpath}.")
            continue

        # Remove file extensions
        filenames = [str(f).split(".")[0] for f in filenames]

        # Files containing coordinate data are processed first
        filenames.sort(key=lambda filename: sort_files(filename, config))

        file_config = config["data_files"][filenames[0]]
        if file_config["coordinates"] == None:
            logger.debug(
                f"None of the files inside {dirpath} have specified coordinate columns.")
            continue

        processed_ids = pd.DataFrame()
        for filename in filenames:
            file_path = Path(dirpath + "/" + filename)

            if filename not in config["data_files"]:
                logger.warning(f"Skipping {file_path}, no config found.")
                continue

            file_config = config["data_files"][filename]
            if file_config["loader"] == None:
                logger.warning(f"Skipping {file_path}, no loader found.")
                continue

            # Select data loader from loaders.py
            loader = getattr(loaders, file_config["loader"])(
                config,
                file_config,
                config["loaders"][file_config["loader"]],
                config["loaders"][file_config["loader"]][filename],
                iteration == dir_count
            )
            logger.debug(f"Loaded loader {loader}")

            if processed_ids.empty:
                processed_data = loader.load_processed_data()
                if not processed_data.empty:
                    processed_ids = processed_data[[file_config["id_column"]]]
            logger.debug(f"{len(processed_ids)} IDs in memory")

            # Select data extractor from extractors.py
            extractor = getattr(extractors, file_config["extractor"])(
                config, file_config, loader)
            logger.debug(f"Loaded extractor {extractor}")

            # Extract data from the file
            logger.info(f"Extracting {file_path}")
            extracted_data = extractor.extract_data(file_path, processed_ids)
            logger.debug(f"Extracted {file_path}")

            if extracted_data.empty:
                logger.info(f"No new data in {file_path}")
                continue

            if file_config["coordinates"] != None:
                if processed_ids.empty:
                    processed_ids = extracted_data[[file_config["id_column"]]]
                else:
                    processed_ids = pd.concat(
                        [processed_ids, extracted_data[[file_config["id_column"]]]])

            # Save extracted data
            logger.debug(f"Loading {len(extracted_data)} entries")
            loader.store_processed_data(extracted_data)
            logger.debug(f"Loaded entries")


if __name__ == "__main__":
    config = load_config()

    # Setup global logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename=config["logs"]["log_file"],
                        filemode=config["logs"]["file_mode"],
                        format="%(asctime)s - %(levelname)s, %(funcName)s(line %(lineno)s): %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        level=get_log_level(config["logs"]["level"]))

    time_start = datetime.datetime.now()
    logger.info(f"Starting script")
    process_data(config)
    logger.info(f"Script finished in {(datetime.datetime.now() - time_start)}")
