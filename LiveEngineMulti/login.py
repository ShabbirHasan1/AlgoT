import constants
import requests
import json
from logging_config import CustomLogger
import constants
from logging_config import CustomLogger, TelegramBot
import time
import pandas as pd
import requests
import json

clientID = "PRO227"
class SymphonyFintechAPI:
    def __init__(self):
        self.api_key = constants.API_KEY
        self.api_secret = constants.API_SECRET
        self.source = constants.SOURCE
        self.base_url = constants.ROOT_URL
        self.token = None

        self.endpoints = {
            'login': '/auth/login',
            'logout': '/auth/logout'
        }

        self.headers = {
            'Content-Type': 'application/json'
        }

        # Initialize logger with filename 'dataEngine'
        self.logger = CustomLogger('dataEngine')

    def update_headers(self, token):
        if token is None:
            self.headers.pop('Authorization', None)
        else:
            self.headers['Authorization'] = token
     
    def write_headers_to_json(self):
        with open('headers.json', 'r') as f:
            data = json.load(f)
            
        data['marketApiHeaders'] = self.headers

        with open('headers.json', 'w') as file:
            json.dump(data, file, indent=4)


    def _make_request(self, endpoint, method='GET', data=None):
        try:
            url = self.base_url + endpoint
            headers = self.headers.copy()

            if method == 'POST':
                response = requests.post(url, headers=headers, json=data)
            elif method == 'GET':
                response = requests.get(url, headers=headers, params=data)
            elif method == 'PUT':
                response = requests.put(url, headers=headers, json=data)
            else:
                raise ValueError("Invalid HTTP method")
            return response
        except Exception as e:
            self.logger.log_message('error', f"Error making {method} request to {url}: {e}")

    def login(self):
        # calling the authenticate endpoint base_url/auth/login
        endpoint = self.endpoints['login']
        data = {
            'secretKey': self.api_secret,
            'appKey': self.api_key,
            'source': self.source
        }
        response = self._make_request(endpoint, method='POST', data=data)
        self.token = response.json()['result']['token']
        self.update_headers(self.token)
        self.logger.log_message('info', "Login successful")
        self.write_headers_to_json()

    def logout(self):
        # calling the authenticate endpoint base_url/auth/logout
        endpoint = self.endpoints['logout']
        response = self._make_request(endpoint, method='POST')
        if response.status_code == 200:
            self.token = None
            self.update_headers(None)
            self.logger.log_message('info', "Logout successful")
        else:
            self.logger.log_message('error', "Error logging out")

class SymphonyInteractiveAPI:
    def __init__(self):
        self.__api_key = constants.INTERACTIVE_API_KEY
        self.__api_secret = constants.INTERACTIVE_API_SECRET
        self.__source = constants.INTERACTIVE_SOURCE
        self.__root_url = constants.INTERACTIVE_ROOT_URL
        self.__token = None

        self.endpoints = {
            "login": ("/interactive/user/session", "POST"),
            "logout": ("/interactive/user/session", "DELETE"),
        }

        self.headers = {"Content-Type": "application/json"}
        self.logger = CustomLogger("loginEngine")

    def update_headers(self, token):
        if token is None:
            self.headers.pop("Authorization", None)
        else:
            self.headers["Authorization"] = token
    
    def write_headers_to_json(self):
        with open('headers.json', 'r') as f:
            data = json.load(f)
        data['orderApiHeaders'] = self.headers

        with open('headers.json', 'w') as file:
            json.dump(data, file, indent=4)

    def _make_request(self, endpoint, data=None):
        try:
            url = self.__root_url + endpoint[0]
            method = endpoint[1]
            headers = self.headers.copy()

            if method == "POST":
                response = requests.post(url, headers=headers, json=data)
            elif method == "GET":
                response = requests.get(url, headers=headers, params=data)
            elif method == "PUT":
                response = requests.put(url, headers=headers, json=data)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers, json=data)
            else:
                raise ValueError("Invalid HTTP method")
            return response
        except Exception as e:
            self.logger.log_message(
                "error", f"Error making request to: {e}"
            )

    def login(self):
        # calling the authenticate endpoint base_url/auth/login
        endpoint = self.endpoints["login"]
        data = {
            "secretKey": self.__api_secret,
            "appKey": self.__api_key,
            "source": self.__source,
        }
        response = self._make_request(endpoint, data=data)
        self.token = response.json()["result"]["token"]
        self.update_headers(self.token)
        self.write_headers_to_json()
        self.logger.log_message("info", "Login Interactive API successful")

    def logout(self):
        try:
            endpoint = self.endpoints["logout"]
            response = self._make_request(endpoint)
            if response.status_code == 200:
                self.token = None
                self.update_headers(self.token)
                self.logger.log_message("info", "Logout successful")
            else:
                self.logger.log_message("error", "Logout failed")
        except Exception as e:
            self.logger.log_message("error", f"Error in logout: {e}")

if __name__ == "__main__":
    api = SymphonyFintechAPI()
    api.login()


    order=SymphonyInteractiveAPI()
    order.login()


