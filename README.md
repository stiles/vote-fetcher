# Vote Fetcher

Vote Fetcher is a simple tool for journalists, researchers, and developers to fetch roll call votes from the US House of Representatives and the Senate. The tool processes the vote data, enriches it with member details, prints summary information to the terminal, and outputs formatted CSV and JSON files for further analysis. Optionally, the outputs can be uploaded to Amazon S3.

## Table of Contents
- [Installation](#installation)
- [Setup](#setup)
- [Usage](#usage)
  - [Fetching House Votes](#fetching-house-votes)
  - [Fetching Senate Votes](#fetching-senate-votes)
- [Output](#output)
- [Examples](#examples)

---

## Installation

1. **Clone the repository:**

    git clone https://github.com/yourusername/vote-fetcher.git  
    cd vote-fetcher

2. **Install dependencies:**

    pip install -r requirements.txt

3. **Optionally, install the package in editable mode:**

    pip install -e .

---

## Setup

Ensure the following directory structure exists for storing output data (the scripts will create them if they don't exist):

    vote-fetcher/
    ├── data/
    │   ├── house/
    │   └── senate/

---

## Usage

The tool includes two scripts:

- `house_votes.py` for fetching House votes.
- `senate_votes.py` for fetching Senate votes.

### Fetching House Votes

To fetch a roll call vote from the House of Representatives, run:

    python house_votes.py --vote_number <vote_number> --year <year> [--bucket <s3_bucket>] [--aws-profile <aws_profile>]

**Arguments**:
- `--vote_number`: The roll call vote number (e.g., `15`).
- `--year`: The year of the vote (e.g., `2025`).
- `--bucket`: (Optional) An S3 bucket name to upload the resulting files.
- `--aws-profile`: (Optional) AWS profile name for authentication.

**Example**:

    python house_votes.py --vote_number 15 --year 2025 --bucket stilesdata.com --aws-profile haekeo

### Fetching Senate Votes

To fetch a roll call vote from the Senate, run:

    python senate_votes.py --congress <congress> --session <session> --vote_number <vote_number> [--bucket <s3_bucket>] [--aws-profile <aws_profile>]

**Arguments**:
- `--congress`: The Congress number (e.g., `119`).
- `--session`: The session number (e.g., `1`).
- `--vote_number`: The roll call vote number (e.g., `15`).  
  *(Note: The vote number is zero-padded to 5 digits in the output.)*
- `--bucket`: (Optional) An S3 bucket name to upload the resulting files.
- `--aws-profile`: (Optional) AWS profile name for authentication.

**Example**:

    python senate_votes.py --congress 119 --session 1 --vote_number 15 --bucket stilesdata.com --aws-profile haekeo

---

## Output

Each script outputs both CSV and JSON files:

- **House Votes:** Files are saved to `data/house/` with filenames based on the year and vote number:
  - Roll call vote: `house_vote_<year>_vote_<vote_number>.csv` and `.json`
  - Partisan summary: `house_partisan_summary_<year>_vote_<vote_number>.csv` and `.json`

- **Senate Votes:** Files are saved to `data/senate/` with filenames based on the congress, session, and vote number (zero-padded to 5 digits):
  - Roll call vote: `senate_vote_<congress>_<session>_vote_<vote_number:05d>.csv` and `.json`
  - Partisan summary: `senate_partisan_summary_<congress>_<session>_vote_<vote_number:05d>.csv` and `.json`

Both scripts print vote summaries to the terminal. For example, the House script prints total member counts, party breakdowns, and vote counts, while the Senate script prints similar details.

If an S3 bucket is specified, the files are also uploaded to S3 using a consistent path pattern.

---

## Examples

### Fetching and Uploading a House Vote

    python house_votes.py --vote_number 15 --year 2025 --bucket stilesdata.com --aws-profile haekeo

Resulting file example (JSON):  
[house_partisan_summary_2025_vote_015.json](https://stilesdata.com/vote-fetcher/house/house_partisan_summary_2025_vote_015.json)

### Fetching and Saving a Senate Vote

    python senate_votes.py --congress 119 --session 1 --vote_number 15 --bucket stilesdata.com --aws-profile haekeo
