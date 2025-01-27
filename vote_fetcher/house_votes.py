#!/usr/bin/env python3

import os
import requests
import pandas as pd
import argparse
from bs4 import BeautifulSoup
import boto3
from datetime import datetime
from utils import save_to_csv, save_to_s3
from data.state_mappings import STATE_ABBREVIATIONS

def fetch_house_member_list():
    """Fetch and process the latest House member list."""
    url = "https://clerk.house.gov/Members/ViewMemberList"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Extract member data
    table = soup.find("table", class_="library-table")
    members = []

    for row in table.find_all("tr")[1:]:
        name = row.find("span", {"data-name": True})
        if name:
            name = name.text.strip()
        party = row.find_all("td")[1].text.strip()
        state_info = row.find_all("td")[2].text.strip()
        state_full = state_info.split(" (")[0]
        district = row.find_all("td")[3].text.strip()
        profile_link = row.find("a")["href"]
        member_id = profile_link.replace("/members/", "")

        # Map full state name to abbreviation
        state_abbreviation = STATE_ABBREVIATIONS.get(state_full.upper(), state_full)

        members.append({
            "name": name,
            "party": party,
            "state": state_abbreviation,
            "district": district,
            "id": member_id
        })

    return pd.DataFrame(members)



def fetch_house_vote(vote_number, year):
    """Fetch and process a House roll call vote."""
    vote_id = f"{year}{vote_number:03d}"
    url = f"https://clerk.house.gov/Votes/{vote_id}?Page=2"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Extract roll call vote table
    table = soup.find_all("table")[1]
    votes = []

    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) >= 5:
            name = cols[0].text.strip()
            vote_result = cols[5].text.strip()
            party_state = cols[0].find("a")
            member_id = party_state["href"].replace("/Members/", "") if party_state else None

            votes.append({
                "name": name,
                "vote": vote_result,
                "id": member_id
            })

    return pd.DataFrame(votes)

def format_final_output(merged_df):
    """Format the merged House vote data with snake_case column names."""
    # Split `name_y` into `last_name` and `first_name`
    merged_df[["last_name", "first_name"]] = merged_df["name_y"].str.split(", ", expand=True)

    # Clean up line breaks and excessive whitespace in names
    def clean_name(name):
        if isinstance(name, str):
            return " ".join(name.split())  # Removes \n and extra spaces
        return name

    merged_df["first_name"] = merged_df["first_name"].apply(clean_name)
    merged_df["last_name"] = merged_df["last_name"].apply(clean_name)

    # Select and reorder relevant columns
    formatted_df = merged_df[
        ["id", "last_name", "first_name", "party", "state", "district", "vote"]
    ].copy()

    # Rename columns to snake_case
    formatted_df.columns = [
        "bioguide_id", "last_name", "first_name", "party", "state", "district", "vote"
    ]

    # Standardize column formats
    formatted_df["party"] = formatted_df["party"].str[0]  # Shorten party names to "D", "R", etc.
    formatted_df["state"] = formatted_df["state"].str.upper()  # Ensure state abbreviations are uppercase

    return formatted_df

def generate_vote_summary(merged_df):
    """Print vote summary with party breakdown, including 'Not Voting'."""
    # Deduplicate by unique identifier
    merged_df = merged_df.drop_duplicates(subset="bioguide_id")

    # Total members
    total_members = merged_df.shape[0]

    # Party breakdown
    party_counts = merged_df["party"].value_counts()
    d_members = party_counts.get("D", 0)
    r_members = party_counts.get("R", 0)
    i_members = party_counts.get("I", 0)

    # Votes breakdown, including "Not Voting"
    vote_counts = merged_df["vote"].value_counts()
    yea_count = vote_counts.get("Yea", 0)
    nay_count = vote_counts.get("Nay", 0)
    not_voting_count = vote_counts.get("Not Voting", 0)

    print("\nVote Summary:")
    print(f"  Total members: {total_members}")
    print(f"  Democrats (D): {d_members}")
    print(f"  Republicans (R): {r_members}")
    print(f"  Independents (I): {i_members}")
    print(f"  Yea votes: {yea_count}")
    print(f"  Nay votes: {nay_count}")
    print(f"  Not Voting: {not_voting_count}")
    print()


def save_to_s3(local_path, bucket, s3_path):
    """Upload file to S3."""
    s3_client = boto3.client("s3")
    s3_client.upload_file(local_path, bucket, s3_path)
    print(f"Uploaded {local_path} to s3://{bucket}/{s3_path}")

def main():
    parser = argparse.ArgumentParser(description="Fetch US House votes and store results.")
    parser.add_argument("--vote_number", required=True, type=int, help="Vote number (e.g., 17)")
    parser.add_argument("--year", required=True, type=int, help="Year of the vote (e.g., 2025)")
    parser.add_argument("--bucket", required=False, help="S3 bucket name (optional)")
    args = parser.parse_args()

    # Fetch and merge data
    print("Fetching House member list...")
    members_df = fetch_house_member_list()

    print("Fetching roll call vote data...")
    votes_df = fetch_house_vote(args.vote_number, args.year)

    print("Merging vote data with member list...")
    merged_df = pd.merge(votes_df, members_df, on="id", how="left")

    # Format the final output
    print("Formatting final output...")
    formatted_df = format_final_output(merged_df)

    # Generate vote summary
    print("Generating vote summary...")
    generate_vote_summary(formatted_df)

    output_dir = os.path.join("..", "data", "house")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"house_vote_{args.year}_vote_{args.vote_number:03d}.csv")
    save_to_csv(formatted_df, output_file)

    # Upload to S3 if specified
    if args.bucket:
        s3_key = f"house_vote_{args.year}_vote_{args.vote_number:03d}.csv"
        save_to_s3(output_file, args.bucket, s3_key)


if __name__ == "__main__":
    main()