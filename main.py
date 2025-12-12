import requests
import time
import json
import sys
import pandas as pd

# --- CONSTANTS ---
API_URL = "http://34.47.186.170/transcribe"
POLL_INTERVAL_SECONDS = 5   # Time to wait between checks
MAX_RETRIES = 20            # Max checks before giving up (20 * 5s = 100s timeout)
CSV_FILE = "call_data_mini.csv"
OUTPUT_FILE = "call_data_processed.csv"

# Headers (Requests handles Content-Type/boundary automatically for multipart)
HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Origin': 'http://34.93.7.14:5173',
    'Referer': 'http://34.93.7.14:5173/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
}

def get_transcription(payload):
    """
    Submits an audio file for transcription and polls until completion.
    """
    print(f"[*] Starting transcription job at {API_URL}...")
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Send POST request (multipart/form-data)
            multipart_data = {k: (None, v) for k, v in payload.items()}
            response = requests.post(API_URL, headers=HEADERS, files=multipart_data, verify=False)
            
            # Check for HTTP errors (4xx, 5xx)
            response.raise_for_status()
            
            # Parse JSON response
            resp_json = response.json()
            
            # Validate Response Structure
            if "Data" not in resp_json or "Status" not in resp_json["Data"]:
                print(f"[!] Invalid response structure: {resp_json}")
                return None

            status = resp_json["Data"]["Status"]
            media_id = resp_json["Data"].get("MediaId", "Unknown")

            # --- LOGIC HANDLER ---
            if status == "Success":
                print(f"\n[+] Job Complete! (Attempt {attempt})")
                print(f"    Media ID: {media_id}")
                print(f"    Transcription URL: {resp_json['Data']['TranscriptionURL']}")
                return resp_json['Data']['TranscriptionURL']
            
            elif status == "Queued":
                print(f"[-] Status: Queued (Attempt {attempt}/{MAX_RETRIES}). Waiting {POLL_INTERVAL_SECONDS}s...")
                time.sleep(POLL_INTERVAL_SECONDS)
                
            else:
                print(f"[!] Unexpected Status: {status}")
                # Optional: break if you encounter a 'Failed' status
                if status == "Failed": 
                    return None
                time.sleep(POLL_INTERVAL_SECONDS)

        except requests.exceptions.RequestException as e:
            print(f"[!] Network Error: {e}")
            time.sleep(POLL_INTERVAL_SECONDS)
            
        except json.JSONDecodeError:
            print(f"[!] Error decoding JSON. Response text: {response.text}")
            break

    print("\n[!] Timeout: Maximum retries reached without success.")
    return None

def process_call_data():
    try:
        print(f"Loading data from {CSV_FILE}...")
        df = pd.read_csv(CSV_FILE)
        
        if df.empty:
            print("CSV is empty.")
            return

        # Ensure column exists
        if 'transcript_url' not in df.columns:
            df['transcript_url'] = None

        print(f"Found {len(df)} rows to process.")

        for index, row in df.iterrows():
            print(f"\n--- Processing Row {index + 1}/{len(df)} ---")
            
            # Construct payload
            try:
                meta_keys = {
                    "receiverId": str(row['seller_identifier']),
                    "callerId": str(row['buyer_identifier']),
                    "modid": str(row['pns_call_modrefname'])
                }
                
                payload = {
                    "callRecordingLink": row['Signed_URL'],
                    "callType": "PNS",
                    "metaKeys": json.dumps(meta_keys)
                }
                
                # Run transcription
                transcript_url = get_transcription(payload)
                
                if transcript_url:
                    df.at[index, 'transcript_url'] = transcript_url
                    print(f"[+] Row {index} updated with transcript URL.")
                else:
                    print(f"[-] Transcription failed for row {index}.")
            
            except Exception as e:
                print(f"[!] Error processing row {index}: {e}")
                continue

            # Save progress after each row
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"Progress saved to {OUTPUT_FILE}")

        print("\nAll rows processed.")

    except FileNotFoundError:
        print(f"Error: {CSV_FILE} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Suppress InsecureRequestWarning if using verify=False
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    process_call_data()