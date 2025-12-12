import requests
import json

class IMOpenAI:
    def __init__(self, api_key, base_url="https://imllm.intermesh.net/v1"):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')

    def chat_completions_create(self, model, messages, **kwargs):
        """
        Mimics openai.chat.completions.create using requests.
        """
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": model,
            "messages": messages
        }
        payload.update(kwargs)
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error calling IMOpenAI: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response text: {e.response.text}")
            return None

    def audio_transcriptions_create(self, model, file, **kwargs):
        """
        Mimics openai.audio.transcriptions.create using requests.
        """
        url = f"{self.base_url}/audio/transcriptions"
        headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        
        # Determine filename
        filename = "audio.mp3"
        if hasattr(file, 'name'):
            filename = file.name
            
        # 'file' argument is expected to be a file-like object
        # We pass a tuple (filename, fileobj, content_type)
        files = {
            "file": (filename, file, "audio/mpeg"),
            "model": (None, model)
        }
        
        # Add other kwargs to files/data
        for k, v in kwargs.items():
            files[k] = (None, str(v))
            
        try:
            response = requests.post(url, headers=headers, files=files, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error calling IMOpenAI Transcription: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response text: {e.response.text}")
            return None
