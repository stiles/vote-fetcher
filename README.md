# Vote Fetcher

Vote Fetcher is a simpe tool for journalists, researchers and developers to fetch roll call votes from the US House of Representatives and Senate. It processes the data, enriches it with member details and outputs formatted CSV files for further analysis. 

## Table of contents
- [Installation](#installation)
- [Setup](#setup)
- [Usage](#usage)
  - [Fetching House Votes](#fetching-house-votes)
  - [Fetching Senate Votes](#fetching-senate-votes)
- [Output](#output)
- [Examples](#examples)

---

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/vote-fetcher.git
   cd vote-fetcher
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Optionally, install the package in editable mode:
   ```bash
   pip install -e .
   ```

---

## Setup

Ensure the following directory structure exists for storing output data:
```
vote-fetcher/
├── data/
│   ├── house/
│   └── senate/
```

This structure is automatically created by the scripts if it doesn't exist.

---

## Usage

The tool includes two scripts:
- `house_votes.py` for fetching House votes.
- `senate_votes.py` for fetching Senate votes.

Run either script with appropriate arguments for your needs:

### Fetching House votes

To fetch a roll call vote from the House of Representatives:
```bash
python house_votes.py --vote_number <vote_number> --year <year>
```

#### Arguments:
- `--vote_number`: The roll call vote number (e.g., `15`).
- `--year`: The year of the vote (e.g., `2025`).
- `--bucket`: (Optional) An S3 bucket name to upload the resulting file.

Example:
```bash
python house_votes.py --vote_number 15 --year 2025
```

### Fetching Senate votes

To fetch a roll call vote from the Senate:
```bash
python senate_votes.py --congress <congress> --session <session> --vote_number <vote_number>
```

#### Arguments:
- `--congress`: The Congress number (e.g., `119`).
- `--session`: The session number (e.g., `1`).
- `--vote_number`: The roll call vote number (e.g., `15`).
- `--bucket`: (Optional) An S3 bucket name to upload the resulting file.

Example:
```bash
python senate_votes.py --congress 119 --session 1 --vote_number 15
```

---

## Output

Each script outputs a CSV file to the corresponding directory under `data/`:

- House votes are saved to `data/house/`.
- Senate votes are saved to `data/senate/`.

The CSV files include detailed member information and vote data. If an S3 bucket is specified, the files are also uploaded to the bucket.

---

## Examples

### Fetching and uploading a House vote
```bash
python house_votes.py --vote_number 15 --year 2025 --bucket my-s3-bucket
```

### Fetching and saving a Senate vote
```bash
python senate_votes.py --congress 119 --session 1 --vote_number 15
```

