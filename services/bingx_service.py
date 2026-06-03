import requests

BASE_URL = "https://open-api.bingx.com"

def test_connection():
    return "BingX connected"

def get_server_time():
    url = f"{BASE_URL}/openApi/swap/v2/server/time"
    response = requests.get(url)
    return response.json()
