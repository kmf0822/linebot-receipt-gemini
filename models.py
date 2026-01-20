import os
from typing import List, Dict

import requests
from src.logger import logger


class ModelInterface:

    def check_token_valid(self) -> bool:
        pass

    def chat_completions(self, messages: List[Dict], model_engine: str) -> str:
        pass

    def audio_transcriptions(self, file, model_engine: str) -> str:
        pass

    def image_generations(self, prompt: str) -> str:
        pass


openai_endpoint = os.getenv('AZURE_OPENAI_ENDPOINT')
openai_api_key = os.getenv("AZURE_OPENAI_API_KEY")
openai_model_engine = os.getenv("AZURE_OPENAI_MODEL_ENGINE")


class OpenAIModel(ModelInterface):
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = openai_endpoint or 'https://openai-workspace-22.openai.azure.com/openai'
        self.api_version = "2025-04-01-preview"
        logger.debug(f"OpenAIModel initialized with base_url: {self.base_url} and api_version: {self.api_version}")

    def _request(self, method, endpoint, body=None, files=None):
        # self.headers = {'Authorization': f'Bearer {self.api_key}'}
        # Content-Type: application/json
        # api-key: YOUR_API_KEY
        self.headers = {}
        try:
            if method == 'GET':
                self.headers['api-key'] = f'{self.api_key}'
                r = requests.get(
                    f'{self.base_url}{endpoint}?api-version={self.api_version}',
                    headers=self.headers)
            elif method == 'POST':
                if body:
                    self.headers['Content-Type'] = 'application/json'
                    self.headers['api-key'] = f'{self.api_key}'
                r = requests.post(
                    f'{self.base_url}{endpoint}?api-version={self.api_version}',
                    headers=self.headers,
                    json=body,
                    files=files)
            r = r.json()
            if r.get('error'):
                return False, None, r.get('error', {}).get('message')
        except Exception:
            return False, None, 'OpenAI API 系統不穩定，請稍後再試'
        return True, r, None

    def check_token_valid(self):
        return self._request('GET', '/models')
        # return self.chat_completions('Hello', os.getenv('OPENAI_MODEL_ENGINE'))

    def chat_completions(self, messages, model_engine) -> str:
        json_body = {'model': model_engine, 'messages': messages}
        return self._request(
            'POST',
            f'/deployments/{openai_model_engine}/chat/completions',
            body=json_body)

    def audio_transcriptions(self, file_path, model_engine) -> str:
        files = {
            'file': open(file_path, 'rb'),
            'model': (None, model_engine),
        }
        return self._request(
            'POST',
            f'/deployments/{openai_model_engine}/audio/transcriptions',
            files=files)

    def image_generations(self, prompt: str) -> str:
        json_body = {"prompt": prompt, "n": 1, "size": "512x512"}
        return self._request(
            'POST',
            f'/deployments/{openai_model_engine}/images/generations',
            body=json_body)
