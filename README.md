# Socrata Scraper

Socrata Scraper is a Python script designed to download and process data assets 
from the open data websites that use the Socrata API. It supports concurrent downloading 
of multiple data assets and logs download activities for transparency and debugging.

## Features

- Fetches and downloads metadata from a Socrata API.
- Processes file and dataset assets based on their type (only tables and blobs).
- Supports concurrent downloading using thread pooling.
- Logs all actions to a specified log file.
- Customizable via command-line arguments.

## Requirements

- Python 3.6 or later

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/libbyh/socrata_scraper.git
   cd socrata_scraper
   ```