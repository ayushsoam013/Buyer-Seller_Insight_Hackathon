from openai_wrapper import IMOpenAI
import os

def test_wrapper():
    # Replace 'sk-xxx' with your actual API key or set env var IM_API_KEY
    api_key = os.getenv("IM_API_KEY", "sk-duxC760Szyz6E6ssm1pmdA")
    
    print(f"Testing IMOpenAI Transcription with key: {api_key[:4]}***")
    
    client = IMOpenAI(api_key=api_key)
    
    file_path = "recording.mp3"
    
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    try:
        with open(file_path, "rb") as audio_file:
            response = client.audio_transcriptions_create(
                model="openai/whisper-1",
                file=audio_file,
                language="hi"
            )
            
            if response:
                print("\nTranscription received:")
                print(response)
            else:
                print("\nFailed to get transcription.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    test_wrapper()
