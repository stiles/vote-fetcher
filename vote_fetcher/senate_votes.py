#!/usr/bin/env python3

import os
import argparse
import requests
import pandas as pd
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import boto3
from datetime import datetime
import unidecode
from utils import save_to_csv, save_to_s3


def fetch_senate_member_list():
    """Fetch and parse the Senate member list from XML feed."""
    url = "https://www.senate.gov/general/contact_information/senators_cfm.xml"
    response = requests.get(url)
    root = ET.fromstring(response.content)

    # Extract data for each senator
    senators = []
    for member in root.findall("member"):
        senators.append({
            "id": member.find("bioguide_id").text,
            "full_name": member.find("member_full").text,
            "last_name": member.find("last_name").text,
            "first_name": member.find("first_name").text,
            "party": member.find("party").text,
            "state": member.find("state").text,
        })

    return pd.DataFrame(senators)


def fetch_senate_vote(congress, session, vote_number):
    """Fetch and parse Senate roll call vote."""
    # Ensure vote_number is zero-padded to 5 digits
    vote_number_padded = f"{int(vote_number):05d}"
    url = f"https://www.senate.gov/legislative/LIS/roll_call_votes/vote{congress}{session}/vote_{congress}_{session}_{vote_number_padded}.htm"
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



def enrich_with_member_data(vote_df, member_df):
    """Enrich vote data with details from the Senate member list."""
    # Normalize and clean names in the roll call vote data
    vote_df["cleaned_name"] = (
        vote_df["name"]
        .str.replace(r"\(.*?\)", "", regex=True)  # Remove party/state info
        .str.replace(r"\s+", " ", regex=True)  # Normalize whitespace
        .str.strip()
        .str.lower()  # Lowercase for consistent comparison
        .apply(unidecode.unidecode)  # Normalize diacritical marks
    )

    # Normalize names in the member list
    member_df["normalized_full_name"] = (
        member_df["full_name"]
        .str.replace(r"\s+", " ", regex=True)  # Normalize whitespace
        .str.strip()
        .str.lower()  # Lowercase for consistent comparison
        .apply(unidecode.unidecode)  # Normalize diacritical marks
    )
    member_df["normalized_last_name"] = (
        member_df["last_name"].str.lower().apply(unidecode.unidecode)
    )

    # Attempt exact match with full_name
    enriched_df = pd.merge(
        vote_df,
        member_df,
        left_on="cleaned_name",
        right_on="normalized_full_name",
        how="left"
    )

    # Fallback to match using last_name
    unmatched = enriched_df[enriched_df["id"].isna()]
    if not unmatched.empty:
        print(f"Trying fallback matching for {len(unmatched)} unmatched rows...")
        unmatched = pd.merge(
            unmatched.drop(columns=member_df.columns),  # Drop member list columns
            member_df,
            left_on="cleaned_name",
            right_on="normalized_last_name",
            how="left",
            suffixes=("", "_alt")
        )
        enriched_df = pd.concat([enriched_df[~enriched_df["id"].isna()], unmatched], ignore_index=True)

    # Final unmatched rows
    final_unmatched = enriched_df[enriched_df["id"].isna()]
    if not final_unmatched.empty:
        print("Warning: Some senators couldn't be matched even after fallback:")
        print(final_unmatched[["name", "cleaned_name"]])

    return enriched_df

def format_final_output(enriched_df):
    """Format the final output with snake_case column names."""
    # Select and reorder relevant columns
    output_df = enriched_df[
        ["id", "full_name", "party", "state", "vote", "cleaned_name"]
    ].copy()

    # Split `full_name` into `last_name` and `first_name`
    output_df[["last_name", "first_name"]] = enriched_df[["last_name", "first_name"]]

    # Rearrange columns for final order
    output_df = output_df[
        ["id", "last_name", "first_name", "party", "state", "vote", "cleaned_name"]
    ]

    # Rename columns to snake_case
    output_df.columns = [
        "id",
        "last_name",
        "first_name",
        "party",
        "state",
        "vote",
        "cleaned_name",
    ]

    return output_df

def generate_vote_summary(merged_df):
    """Generate a summary of the vote, ensuring no duplicates and accurate totals."""
    # Drop duplicate rows based on unique identifiers
    merged_df = merged_df.drop_duplicates(subset="id")

    # Total members
    total_members = merged_df.shape[0]

    if total_members != 100:
        print(f"Warning: Unexpected total members count ({total_members}) after deduplication.")
        print("Debugging unique IDs and rows for duplicates...\n")
        print(merged_df["id"].value_counts())
        return

    # Party breakdown
    party_counts = merged_df["party"].value_counts()
    d_members = party_counts.get("D", 0)
    r_members = party_counts.get("R", 0)
    i_members = party_counts.get("I", 0)

    # Democrats + Independents
    d_with_i = d_members + i_members

    # Votes breakdown, including "Not Voting"
    vote_counts = merged_df["vote"].value_counts()
    yea_count = vote_counts.get("Yea", 0)
    nay_count = vote_counts.get("Nay", 0)
    not_voting_count = vote_counts.get("Not Voting", 0)

    print("\nVote Summary:")
    print(f"  Total members: {total_members}")
    print(f"  Democrats + Independents (D + I): {d_with_i}")
    print(f"  Republicans (R): {r_members}")
    print(f"  Yea votes: {yea_count}")
    print(f"  Nay votes: {nay_count}")
    print(f"  Not Voting: {not_voting_count}")


def generate_partisan_summary(enriched_df, output_dir, congress, session, vote_number):
    """Generate a detailed partisan summary of the vote and save as a CSV."""
    enriched_df = enriched_df.drop_duplicates(subset='id')
    
    overall = (
        enriched_df.groupby(["party", "vote"])
        .size()
        .reset_index(name="count")
        .pivot_table(columns="party", values="count", index="vote")
        .reset_index()
        .rename(columns={"D": "Democratic", "R": "Republican", "I": "Independent"})
        .sort_values("vote", ascending=False)
        .fillna(0)
    )

    # Calculate total votes for 'Yea' and 'Nay'
    yea_votes = overall[overall["vote"] == "Yea"].iloc[:, 1:].sum(axis=1).values[0]
    nay_votes = overall[overall["vote"] == "Nay"].iloc[:, 1:].sum(axis=1).values[0]

    # Determine the winning side and append '✓' to the winning vote
    if yea_votes > nay_votes:
        overall.loc[overall["vote"] == "Yea", "vote"] += " ✓"
    else:
        overall.loc[overall["vote"] == "Nay", "vote"] += " ✓"

    # Save the partisan summary as a CSV
    summary_file = os.path.join(output_dir, f"senate_partisan_summary_{congress}_{session}_vote_{vote_number:05d}.csv")
    save_to_csv(overall, summary_file)

    # Print the partisan summary
    print("\nPartisan Summary:")
    print(overall)


def main():
    parser = argparse.ArgumentParser(description="Fetch US Senate votes and store results.")
    parser.add_argument("--congress", required=True, type=int, help="Congress number (e.g., 119)")
    parser.add_argument("--session", required=True, type=int, help="Session number (e.g., 1)")
    parser.add_argument("--vote_number", required=True, type=int, help="Vote number (e.g., 15)")
    parser.add_argument("--bucket", required=False, help="S3 bucket name (optional)")
    args = parser.parse_args()

    # Fetch member list
    print("Fetching Senate member list...")
    member_df = fetch_senate_member_list()

    # Fetch roll call vote data
    print("Fetching Senate roll call vote...")
    vote_df = fetch_senate_vote(args.congress, args.session, args.vote_number)
    if vote_df is None:
        print("No vote data available. Exiting.")
        return

    # Enrich vote data with member details
    print("Enriching vote data with member details...")
    enriched_df = enrich_with_member_data(vote_df, member_df)

    # Print vote summary
    print("Generating vote summary...")
    generate_vote_summary(enriched_df)

    # Format the enriched data for saving
    print("Formatting final output...")
    formatted_df = format_final_output(enriched_df)

    # Generate and print vote summary
    print("Generating vote summary...")
    generate_vote_summary(enriched_df)

    output_dir = os.path.join("..", "data", "senate")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"senate_vote_{args.congress}_{args.session}_vote_{args.vote_number:05d}.csv")
    save_to_csv(formatted_df, output_file)

    # Generate and save partisan summary
    print("Generating partisan summary...")
    generate_partisan_summary(enriched_df, output_dir, args.congress, args.session, args.vote_number)

    # Upload to S3 if specified
    if args.bucket:
        s3_key = f"senate_vote_{args.congress}_{args.session}_vote_{args.vote_number:05d}.csv"
        save_to_s3(output_dir, args.bucket, s3_key)



if __name__ == "__main__":
    main()