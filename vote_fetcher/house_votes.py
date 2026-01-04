#!/usr/bin/env python3
import os
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import argparse

from utils import save_to_csv, save_to_s3
from data.state_mappings import STATE_ABBREVIATIONS

def fetch_roll_call_vote(vote_number: str, year: str) -> pd.DataFrame:
    """Fetch and parse the roll call vote table from the clerk page."""
    vote_id = f"{year}{vote_number}"
    vote_url = f"https://clerk.house.gov/Votes/{vote_id}?Page=2"
    response = requests.get(vote_url)
    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find_all("table")[1]  # Assumes the roll call vote table is the 2nd table.
    reps, ids, parties, states, votes = [], [], [], [], []
    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) >= 5:
            reps.append(cols[0].get_text().strip())
            link = cols[0].find("a")
            ids.append(link["href"].replace("/Members/", "") if link else None)
            parties.append(cols[2].get_text().strip())
            states.append(cols[3].get_text().strip())
            votes.append(cols[5].get_text().strip())
    df = pd.DataFrame({
        "name": reps,
        "id": ids,
        "party": parties,
        "state": states,
        "vote": votes,
    })
    # Clean vote values.
    df["vote_clean"] = df["vote"].replace({"Yea": "Yes", "Nay": "No", "Aye": "Yes"})
    df["party_letter"] = df["party"].str[:1]
    # Use a raw string to escape the parenthesis.
    df["name_clean"] = df["name"].str.split(r" \(", expand=True)[0]
    return df

def fetch_members_list() -> pd.DataFrame:
    """Fetch the live House members list from the clerk's website."""
    url = "https://clerk.house.gov/Members/ViewMemberList"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table", class_="library-table")
    
    representatives, parties, states, districts, profile_links = [], [], [], [], []
    for row in table.find_all("tr")[1:]:
        name_span = row.find("span", {"data-name": True})
        if name_span:
            name = name_span.text.strip()
        else:
            name_span_hidden = row.find("span", class_="name")
            name = name_span_hidden.text.strip() if name_span_hidden else None
        party = row.find("td").find_next_sibling("td").get_text(strip=True)
        state_info = row.find_all("td")[2].get_text(strip=True)
        state, _ = state_info.split(" (")
        district = row.find_all("td")[3].get_text(strip=True)
        profile_link = row.find("a")["href"]

        representatives.append(name)
        parties.append(party)
        states.append(state)
        districts.append(district)
        profile_links.append(profile_link)
    
    df = pd.DataFrame({
        "representative": representatives,
        "party": parties,
        "state": states,
        "district": districts,
        "profile_link": profile_links,
    })
    df[["last_name", "first_name"]] = df["representative"].str.split(", ", n=1, expand=True)
    df["id"] = df["profile_link"].str.replace("/members/", "")
    df["abbreviation"] = df["state"].str.upper().map(STATE_ABBREVIATIONS).fillna("")
    df = df[["first_name", "last_name", "representative", "party", "state", "abbreviation", "district", "id"]]
    return df

def merge_votes_with_members(votes_df: pd.DataFrame, members_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge roll call vote data with the live members list.
    If a vote row doesn't match a member, default to the clerk's data.
    """
    merge = pd.merge(
        votes_df,
        members_df[["id", "representative", "district"]],
        on="id",
        how="left",
        indicator=True,
    )
    merge["representative"] = merge["representative"].combine_first(merge["name"])
    merge["district"] = merge["district"].fillna("")
    df_clean = merge[["name", "representative", "state", "district", "party_letter", "vote_clean"]].fillna("")
    df_clean.columns = ["Name", "Representative", "State", "District", "Party", "Vote"]
    return df_clean

def fetch_overall_summary(vote_number: str, year: str) -> pd.DataFrame:
    """Fetch and process the overall partisan summary table from the clerk page."""
    vote_id = f"{year}{vote_number}"
    vote_url = f"https://clerk.house.gov/Votes/{vote_id}?Page=2"
    overall = pd.read_html(vote_url)[0]
    overall = overall.rename(
        columns={"Yeas": "Yes", "Nays": "No", "Ayes": "Yes", "Noes": "No"}
    ).drop([2, 3])
    overall["Not voting/present"] = overall["Present"] + overall["Not Voting"]
    overall_pivot = (
        overall.pivot_table(
            values=["No", "Yes", "Not voting/present"],
            columns="Party",
        )
        .reset_index()
        .rename(columns={"index": "Vote"})
    )
    # Mark the winning side with a check.
    yes_votes = overall_pivot[overall_pivot["Vote"] == "Yes"].iloc[:, 1:].sum(axis=1).values[0]
    no_votes = overall_pivot[overall_pivot["Vote"] == "No"].iloc[:, 1:].sum(axis=1).values[0]
    if yes_votes > no_votes:
        overall_pivot.loc[overall_pivot["Vote"] == "Yes", "Vote"] += " ✓"
    else:
        overall_pivot.loc[overall_pivot["Vote"] == "No", "Vote"] += " ✓"
    overall_pivot.columns = ["Vote"] + [col[1] if isinstance(col, tuple) else col for col in overall_pivot.columns[1:]]
    if {"Democratic", "Republican"}.issubset(overall_pivot.columns):
        overall_pivot["total"] = overall_pivot[["Democratic", "Republican"]].sum(axis=1)
        overall_pivot = overall_pivot.sort_values("total", ascending=False)
    return overall_pivot

def export_json(df: pd.DataFrame, filepath: str):
    """Export DataFrame to a JSON file with non-ASCII characters."""
    df.to_json(filepath, indent=4, orient="records", force_ascii=False)

def generate_vote_summary(merged_df: pd.DataFrame):
    """Print a summary of the House vote to the terminal."""
    df = merged_df.drop_duplicates(subset="Name")
    total_members = df.shape[0]
    party_counts = df["Party"].value_counts()
    vote_counts = df["Vote"].value_counts()
    yes_count = vote_counts.get("Yes", 0)
    no_count = vote_counts.get("No", 0)
    not_voting_count = vote_counts.get("Not Voting", 0)
    
    print("\nHouse Vote Summary:")
    print(f"  Total members: {total_members}")
    print(f"  Democrats: {party_counts.get('D', 0)}")
    print(f"  Republicans: {party_counts.get('R', 0)}")
    print(f"  Independents: {party_counts.get('I', 0)}")
    print(f"  Yes: {yes_count}")
    print(f"  No: {no_count}")
    print(f"  Not Voting: {not_voting_count}")

def main():
    parser = argparse.ArgumentParser(description="Fetch US House votes and export results.")
    parser.add_argument("--vote_number", required=True, type=int, help="Vote number (e.g., 15)")
    parser.add_argument("--year", required=True, type=int, help="Year of the vote (e.g., 2025)")
    parser.add_argument("--bucket", help="S3 bucket name (optional)")
    parser.add_argument("--aws-profile", dest="aws_profile", help="AWS profile name (optional)")
    args = parser.parse_args()

    vote_number = f"{args.vote_number:03d}"
    year = str(args.year)
    # File names built using year and vote number.
    roll_call_filename = f"house_vote_{year}_vote_{vote_number}"
    partisan_filename = f"house_partisan_summary_{year}_vote_{vote_number}"
    
    # Save files in ../data/house (one level up from vote_fetcher)
    output_dir = os.path.join("..", "data", "house")
    os.makedirs(output_dir, exist_ok=True)
    
    roll_call_json = os.path.join(output_dir, f"{roll_call_filename}.json")
    overall_json = os.path.join(output_dir, f"{partisan_filename}.json")
    roll_call_csv = os.path.join(output_dir, f"{roll_call_filename}.csv")
    overall_csv = os.path.join(output_dir, f"{partisan_filename}.csv")
    
    print("Fetching roll call vote data...")
    votes_df = fetch_roll_call_vote(vote_number, year)
    
    print("Fetching live House members list...")
    members_df = fetch_members_list()
    
    print("Merging vote data with members list...")
    merged_df = merge_votes_with_members(votes_df, members_df)
    
    # Print vote summary to terminal.
    generate_vote_summary(merged_df)
    
    print("\nFetching overall partisan summary...")
    overall_summary = fetch_overall_summary(vote_number, year)
    print("\nOverall Partisan Summary:")
    print(overall_summary)
    
    print("\nExporting roll call vote data to JSON and CSV...")
    export_json(merged_df, roll_call_json)
    save_to_csv(merged_df, roll_call_csv)
    
    print("\nExporting overall partisan summary to JSON and CSV...")
    export_json(overall_summary, overall_json)
    save_to_csv(overall_summary, overall_csv)
    
    subdirectory = "vote-fetcher/house"
    if args.bucket:
        for local_file, s3_key in [
            (roll_call_csv, f"{subdirectory}/{roll_call_filename}.csv"),
            (roll_call_json, f"{subdirectory}/{roll_call_filename}.json"),
            (overall_csv, f"{subdirectory}/{partisan_filename}.csv"),
            (overall_json, f"{subdirectory}/{partisan_filename}.json"),
        ]:
            save_to_s3(local_file, args.bucket, s3_key, args.aws_profile)
    
    print("\nAll done!")

if __name__ == "__main__":
    main()