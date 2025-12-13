import pandas as pd
import requests

def fetch_transcript_text(url):
    """
    Fetches the text content from the given URL.
    Returns the text if successful, or an empty string/error message if not.
    """
    if not isinstance(url, str) or not url.startswith('http'):
        return ""
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return ""

def main():
    input_file = 'call_data_processed.csv'
    output_file = 'Hackathon _Audio_Data.csv'

    try:
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} rows from {input_file}")
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
        return

    if 'transcript_url' not in df.columns:
        print("Error: 'transcript_url' column not found in the CSV.")
        return

    print("Fetching transcripts...")
    # Apply the fetch function to the transcript_url column
    df['transcriped_text'] = df['transcript_url'].apply(fetch_transcript_text)

    # Save the updated DataFrame
    df.to_csv(output_file, index=False)
    print(f"Updated CSV saved to {output_file}")

if __name__ == "__main__":
    main()
