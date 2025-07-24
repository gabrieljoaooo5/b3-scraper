# B3 Scraper

This project scrapes data from B3 (Brasil Bolsa Balcão), processes it, and uploads it to S3 in Parquet format.

## Features

- Extracts portfolio data from B3 using base64-encoded API payloads
- Saves data in Parquet formats
- Uploads results to Amazon S3
- Supports `.env` configuration

## Requirements

- Python 3.9 or higher
- [Poetry](https://python-poetry.org/) for dependency management

## Installation

Clone the repository:

```bash
git clone https://github.com/gabrieljoaooo5/b3-scraper.git
cd b3-scraper
