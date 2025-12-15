import requests
import time
import pandas as pd
import concurrent.futures
import threading
import urllib3

# --- CONSTANTS ---
API_URL = "http://34.100.247.189/transcribe"
POLL_INTERVAL_SECONDS = 5
MAX_RETRIES = 20
CSV_FILE = "call_data.csv"
OUTPUT_FILE = "call_data_processed_threaded.csv"
MAX_WORKERS = 5  # Adjust based on system/network capabilities

# Headers
HEADERS = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Origin': 'http://34.93.7.14:5173',
    'Referer': 'http://34.93.7.14:5173/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
}

# Lock for thread-safe file writing
file_lock = threading.Lock()

def get_transcription(payload, row_index):
    """
    Submits an audio file for transcription and polls until completion.
    """
    print(f"[*] [Row {row_index}] Starting transcription job...")
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Send POST request
            multipart_data = {k: (None, v) for k, v in payload.items()}
            response = requests.post(API_URL, headers=HEADERS, files=multipart_data, verify=False)
            response.raise_for_status()
            
            resp_json = response.json()
            
            if "Data" not in resp_json or "Status" not in resp_json["Data"]:
                print(f"[!] [Row {row_index}] Invalid response structure.")
                return None

            status = resp_json["Data"]["Status"]
            
            if status == "Success":
                print(f"[+] [Row {row_index}] Success! URL: {resp_json['Data']['TranscriptionURL']}")
                return resp_json['Data']['TranscriptionURL']
            
            elif status == "Queued":
                # print(f"[-] [Row {row_index}] Queued (Attempt {attempt})...")
                time.sleep(POLL_INTERVAL_SECONDS)
                
            elif status == "Failed":
                print(f"[!] [Row {row_index}] Failed.")
                return None
            else:
                time.sleep(POLL_INTERVAL_SECONDS)

        except Exception as e:
            print(f"[!] [Row {row_index}] Error: {e}")
            time.sleep(POLL_INTERVAL_SECONDS)
            
    print(f"[!] [Row {row_index}] Timeout.")
    return None

def process_row(index, row, df):
    """
    Process a single row: prepare payload, get transcription, update DF.
    """
    try:
        # Construct payload
        payload = {
            "caller_id": str(row['buyer_identifier']),
            "receiver_id": str(row['seller_identifier']),
            "callRecordingLink": row['Signed_URL'],
            "callType": "PNS"
        }
        
        transcript_url = get_transcription(payload, index)
        
        if transcript_url:
            # Update DataFrame
            df.at[index, 'transcript_url'] = transcript_url
            
            # Save progress safely
            with file_lock:
                df.to_csv(OUTPUT_FILE, index=False)
                # print(f"[Saved] Progress updated for Row {index}")
        else:
            print(f"[-] [Row {index}] No transcript URL retrieved.")

    except Exception as e:
        print(f"[!] [Row {index}] Critical Error: {e}")

def main():
    # Suppress warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    print(f"Loading data from {CSV_FILE}...")
    try:
        df = pd.read_csv(CSV_FILE)
    except FileNotFoundError:
        print(f"Error: {CSV_FILE} not found.")
        return

    if df.empty:
        print("CSV is empty.")
        return

    # Ensure column exists
    if 'transcript_url' not in df.columns:
        df['transcript_url'] = None

    print(f"Starting processing for {len(df)} rows with {MAX_WORKERS} threads...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for index, row in df.iterrows():
            # Submit task
            futures.append(executor.submit(process_row, index, row, df))
        
        # Wait for all to complete
        concurrent.futures.wait(futures)

    print("\nAll rows processed.")

if __name__ == "__main__":
    main()
