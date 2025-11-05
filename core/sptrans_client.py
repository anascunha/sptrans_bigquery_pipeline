# sptrans_client.py
import requests

class SPTransClient:
    def __init__(self, base_url, token, proxies=None):
        self.base_url = base_url
        self.token = token
        self.proxies = proxies
        self.session = requests.Session()

    def authenticate(self):
        auth_url = f"{self.base_url}/Login/Autenticar?token={self.token}"
        response = self.session.post(auth_url, proxies=self.proxies)
        if response.text.strip().lower() == "true":
            print("Autenticação bem-sucedida.")
            return True
        else:
            raise Exception(f"Falha na autenticação: {response.text}")

    def get_posicao(self):
        url = f"{self.base_url}/Posicao"
        response = self.session.get(url, proxies=self.proxies)
        if response.status_code == 200:
            return response.json()
        elif response.status_code in [401, 403]:
            print("Sessão expirada. Reautenticando...")
            self.authenticate()
            return self.get_posicao()
        else:
            raise Exception(f"Erro ao acessar /Posicao: {response.status_code} - {response.text}")