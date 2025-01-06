# Pokémon Data Pipeline with Google Cloud Platform

## Overview
This script fetches Pokémon data from an external API, transforms it into a structured format, and stores the data in Google BigQuery for further analysis. It is designed to be used with Google Cloud Functions, with optional configurations for local testing.

## Features
- **Fetch Data**: Retrieves Pokémon data from the [Pokémon Showdown API](https://play.pokemonshowdown.com/data/pokedex.json).
- **Data Transformation**: Cleans and restructures the raw data, including:
  - Handling gender ratios.
  - Skipping invalid entries (e.g., entries with negative `dexnum`).
  - Parsing various Pokémon attributes.
- **BigQuery Integration**: Uploads a daily snapshot of the transformed data to a BigQuery table for storage and analysis.

## Prerequisites
- A Google Cloud Platform (GCP) project with:
  - Google Cloud Storage enabled (optional).
  - BigQuery dataset and table set up.
- Python 3.x installed.
- Required Python libraries:
  - `requests`
  - `google-cloud-storage`
  - `google-cloud-bigquery`
- A service account with appropriate permissions for Storage and BigQuery.

## Setup Instructions
### 1. Clone the Repository
```bash
git clone <repository-url>
cd <repository-folder>
