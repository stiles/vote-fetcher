#!/usr/bin/env python3

import os
import argparse
import requests
import pandas as pd
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import unidecode
from src.utils import save_to_csv, save_to_s3, normalize_name
from src.data.state_mappings import STATE_ABBREVIATIONS
import sys

# Add the src directory to the system path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .utils import save_to_csv, save_to_s3, normalize_name


# --- Senate-Specific Functions ---
def fetch_senate_member_list():
    """Fetch and parse the Senate member list from XML feed."""
    url = "https://www.senate.gov/general/contact_information/senators_cfm.xml"
    response = requests.get(url)
    root = ET.fromstring(response.content)

    senators = [
        {
            "id": member.find("bioguide_id").text,
            "full_name": member.find("member_full").text,
            "last_name": member.find("last_name").text,
            "first_name": member.find("first_name").text,
            "party": member.find("party").text,
            "state": member.find("state").text,
        }
        for member in root.findall("member")
    ]
    return pd.DataFrame(senators)

def fetch_senate_vote(congress, session, vote_num):
    """Fetch and parse Senate roll call vote."""
    url = f"https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{vote_num}.htm"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    if "Unavailable" in soup.title.get_text():
        print("Roll call vote data is currently unavailable.")
        return None

    data_div = soup.find("div", class_="newspaperDisplay_3column")
    vote_data = data_div.find("span", class_="contenttext").get_text()

    votes = []
    for line in vote_data.split("\n"):
        if not line.strip():
            continue
        parts = line.split(", ")
        votes.append({
            "name": parts[0].strip(),
            "vote": parts[1].strip() if len(parts) > 1 else None,
        })

    return pd.DataFrame(votes)

# --- House-Specific Functions ---
def fetch_house_member_list():
    """Fetch and parse the House member list."""
    url = "https://clerk.house.gov/Members/ViewMemberList"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    table = soup.find("table", class_="library-table")
    members = []
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        name = row.find("span", {"data-name": True}).text.strip()
        party = cols[1].text.strip()[0]
        state = cols[2].text.strip().split(" (")[0]
        district = cols[3].text.strip()
        member_id = row.find("a")["href"].replace("/members/", "")

        members.append({"id": member_id, "name": name, "party": party, "state": state, "district": district})

    return pd.DataFrame(members)

def fetch_house_vote(vote_number, year):
    """Fetch and parse House roll call vote."""
    vote_id = f"{year}{vote_number:03d}"
    url = f"https://clerk.house.gov/Votes/{vote_id}"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    table = soup.find_all("table")[1]
    votes = []
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        votes.append({
            "name": cols[0].text.strip(),
            "vote": cols[5].text.strip(),
        })

    return pd.DataFrame(votes)

# --- Main Entrypoints ---
def process_vote(fetch_member_list, fetch_vote, congress=None, session=None, vote_num=None, year=None, chamber=None, bucket=None):
    """Generalized processing function for fetching and saving votes."""
    print(f"Fetching {chamber} member list...")
    member_df = fetch_member_list()

    print(f"Fetching {chamber} roll call vote...")
    if chamber == "senate":
        vote_df = fetch_vote(congress, session, vote_num)
    elif chamber == "house":
        vote_df = fetch_vote(vote_num, year)
    else:
        raise ValueError("Chamber must be 'senate' or 'house'.")

    if vote_df is None:
        print("No vote data available. Exiting.")
        return

    # Join dataframes
    vote_df["cleaned_name"] = vote_df["name"].apply(normalize_name)
    member_df["normalized_name"] = member_df["full_name"].apply(normalize_name)
    merged_df = pd.merge(vote_df, member_df, left_on="cleaned_name", right_on="normalized_name", how="left")

    # Summarize vote
    total_members_expected = 100 if chamber == "senate" else 435
    generate_vote_summary(merged_df, total_members_expected)

    # Save data
    if chamber == "senate":
        file_name = f"votes/senate_{congress}_{session}_vote_{int(vote_num):05d}.csv"
    else:
        file_name = f"votes/house_{year}_{vote_num:03d}.csv"

    save_to_csv(merged_df, file_name)

    if bucket:
        save_to_s3(file_name, bucket, file_name)

def main():
    parser = argparse.ArgumentParser(description="Fetch US Congressional votes.")
    parser.add_argument("--chamber", required=True, help="Chamber: 'house' or 'senate'")
    parser.add_argument("--congress", type=int, help="Congress number (e.g., 119 for Senate)")
    parser.add_argument("--session", type=int, help="Session number (e.g., 1 for Senate)")
    parser.add_argument("--vote_num", type=int, help="Vote number (e.g., 15)")
    parser.add_argument("--year", type=int, help="Year of the vote (e.g., 2025 for House)")
    parser.add_argument("--bucket", help="S3 bucket name (optional)")
    args = parser.parse_args()

    if args.chamber == "senate":
        process_vote(
            fetch_senate_member_list,
            fetch_senate_vote,
            congress=args.congress,
            session=args.session,
            vote_num=args.vote_num,
            chamber="senate",
            bucket=args.bucket,
        )
    elif args.chamber == "house":
        process_vote(
            fetch_house_member_list,
            fetch_house_vote,
            year=args.year,
            vote_num=args.vote_num,
            chamber="house",
            bucket=args.bucket,
        )
    else:
        print("Invalid chamber specified. Use 'house' or 'senate'.")

if __name__ == "__main__":
    main()