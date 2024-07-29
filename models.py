from typing import List, Dict
import requests
import os


class ModelInterface:

    def check_token_valid(self) -> bool:
        pass

    def chat_completions(self, messages: List[Dict], model_engine: str) -> str:
        pass

    def audio_transcriptions(self, file, model_engine: str) -> str:
        pass

    def image_generations(self, prompt: str) -> str:
        pass


class OpenAIModel(ModelInterface):

    def __init__(self, api_key: str):
        self.api_key = api_key
        # self.api_key = "73ba3ef13d7f4d38a66ab055797570e7"
        # self.base_url = 'https://api.openai.com/v1'

        self.base_url = 'https://openai-workspace-22.openai.azure.com/openai'
        # self.base_url = 'https://openai-workspace-22.openai.azure.com/openai/deployments/gpt-4-8K'
        # self.api_type = "azure"
        # self.api_base = "https://openai-workspace-22.openai.azure.com/"
        self.api_version = "2024-05-01-preview"
        # self.api_version = "2023-07-01-preview"
        # self.api_key = os.getenv("OPENAI_API_KEY")
        # https://openai-workspace-22.openai.azure.com/openai/deployments/gpt-4-8K/chat/completions?api-version=2023-07-01-preview

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
            f'/deployments/{os.getenv("OPENAI_MODEL_ENGINE")}/chat/completions',
            body=json_body)

    def audio_transcriptions(self, file_path, model_engine) -> str:
        files = {
            'file': open(file_path, 'rb'),
            'model': (None, model_engine),
        }
        return self._request(
            'POST',
            f'/deployments/{os.getenv("OPENAI_MODEL_ENGINE")}/audio/transcriptions',
            files=files)

    def image_generations(self, prompt: str) -> str:
        json_body = {"prompt": prompt, "n": 1, "size": "512x512"}
        return self._request(
            'POST',
            f'/deployments/{os.getenv("OPENAI_MODEL_ENGINE")}/images/generations',
            body=json_body)
