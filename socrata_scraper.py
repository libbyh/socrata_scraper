import requests
import json
import argparse
from datetime import datetime
import time
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

class SocrataDownloader:
    def __init__(self, base_dir="cdc_data", log_file="download_log.txt", api_base_url="https://data.cdc.gov/api", download_base_url="https://data.cdc.gov/download"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        self.api_base_url = api_base_url
        self.download_base_url = download_base_url

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.base_dir / log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging

    def download_metadata(self):
        try:
            url = f"{self.api_base_url}/views/metadata/v1"
            response = requests.get(url)
            response.raise_for_status()

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"metadata_{timestamp}.json"

            metadata_path = self.base_dir / filename
            with open(metadata_path, 'w') as f:
                json.dump(response.json(), f)

            self.logger.info(f"Downloaded metadata to {metadata_path}")
            return metadata_path

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error downloading metadata: {e}")
            raise

    def get_asset_details(self, asset_id):
        try:
            url = f"{self.api_base_url}/views/{asset_id}"
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error getting details for asset {asset_id}: {e}")
            return None

    def download_file_asset(self, asset_id, asset_details, retries=3, delay=1):
        for attempt in range(retries):
            try:
                blob_mime_type = asset_details.get('blobMimeType')
                if not blob_mime_type:
                    self.logger.warning(f"No blobMimeType found for file asset {asset_id}")
                    return

                file_url = f"{self.download_base_url}/{asset_id}/{blob_mime_type}"

                response = requests.get(file_url, stream=True)
                response.raise_for_status()

                filename = asset_details.get('blobFilename')
                if not filename:
                    filename = f"{asset_id}.file"
                    self.logger.warning(f"blobFilename missing for asset {asset_id}. Using {filename} as filename.")

                filepath = self.base_dir / filename

                if filepath.exists():
                    name = filepath.stem
                    suffix = filepath.suffix
                    filepath = self.base_dir / f"{name}_{asset_id}{suffix}"
                    self.logger.warning(f"File {filename} already exists. Renaming to {filepath}.")

                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                self.logger.info(f"Downloaded file asset {asset_id} to {filepath}")
                return

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error downloading file asset {asset_id} (attempt {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(delay * (2**attempt))
                continue

        self.logger.error(f"Failed to download file asset {asset_id} after {retries} retries.")


    def download_table_asset(self, asset_id):
        try:
            url = f"{self.api_base_url}/views/{asset_id}/rows.csv"
            response = requests.get(url, stream=True)
            response.raise_for_status()

            filepath = self.base_dir / f"{asset_id}.csv"
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            self.logger.info(f"Downloaded table asset {asset_id} to {filepath}")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error downloading table asset {asset_id}: {e}")

    def process_assets(self, metadata_path, max_concurrent_downloads=3):
        try:
            with open(metadata_path) as f:
                metadata_list = json.load(f)

            if not isinstance(metadata_list, list):
                self.logger.error("Metadata file does not contain a list.")
                return

            asset_ids = []
            for item in metadata_list:
                if isinstance(item, dict):
                    asset_id = item.get('id')
                    if asset_id:
                        asset_ids.append(asset_id)
                    else:
                        self.logger.warning("Item in the list does not contain an 'id' key.")
                else:
                    self.logger.warning("Item in the list is not a dictionary.")

            self.logger.info(f"Found {len(asset_ids)} assets to process.")

            with ThreadPoolExecutor(max_workers=max_concurrent_downloads) as executor:
                futures = [executor.submit(self.process_single_asset, asset_id) for asset_id in asset_ids]

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            self.logger.info(result)
                    except Exception as e:
                        self.logger.error(f"A thread raised an exception: {e}")

        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding metadata JSON: {e}")
        except Exception as e:
            self.logger.error(f"Error processing metadata file: {e}")
            raise

    def process_single_asset(self, asset_id):
        details_path = self.base_dir / f"{asset_id}_metadata.json"
        if details_path.exists():
            self.logger.info(f"Metadata file already exists for asset {asset_id}. Skipping.")
            return f"Asset {asset_id} metadata already exists. Skipped."

        try:
            asset_details = self.get_asset_details(asset_id)
            if not asset_details:
                return f"No details found for asset {asset_id}"

            with open(details_path, 'w') as f:
                json.dump(asset_details, f)

            asset_type = asset_details.get('assetType', '')
            if asset_type == 'file':
                self.download_file_asset(asset_id, asset_details)
            elif asset_type == 'dataset':
                self.download_table_asset(asset_id)
            else:
                self.logger.warning(f"Unknown asset type {asset_type} for {asset_id}")

            return f"Asset {asset_id} processed successfully."

        except Exception as e:
            self.logger.error(f"Error processing asset {asset_id}: {e}")
            return f"Error processing asset {asset_id}: {e}"


def parse_args():
    parser = argparse.ArgumentParser(description='Download CDC data assets')
    parser.add_argument('--output-dir', type=str, default='cdc_data', help='Directory to store downloaded assets')
    parser.add_argument('--log-file', type=str, default='socrata_downloader_log.txt', help='Filename for logging')
    parser.add_argument('--concurrency', type=int, default=3, help='Number of concurrent downloads')
    parser.add_argument('--api-url', type=str, default='https://data.cdc.gov', help='Base URL for the Socrata API')
    return parser.parse_args()


def main():
    try:
        args = parse_args()
        api_url = args.api_url + "/api"
        download_url = args.api_url + "/download"
        downloader = SocrataDownloader(base_dir=args.output_dir, log_file=args.log_file, api_base_url=api_url, download_base_url=download_url)
        metadata_path = downloader.download_metadata()
        downloader.process_assets(metadata_path, max_concurrent_downloads=args.concurrency)

    except Exception as e:
        logging.error(f"Fatal error in main process: {e}")
        raise


if __name__ == "__main__":
    main()