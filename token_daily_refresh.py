import os
import pickle
import logging
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

logger = logging.getLogger(__name__)


SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

def get_or_create_token():
    """
    Получает или создает токен:
    1. Если файла нет - открывает браузер для аутентификации
    2. Если токен просрочен - обновляет его
    """
    creds = None
    
    # Если файл существует, загружаем токен
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    
    # Если токена нет или он невалиден
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Обновляем просроченный токен
            creds.refresh(Request())
        else:
            # Полная аутентификация
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Сохраняем токен
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return creds


def refresh_token():
    try:
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
        
        creds.refresh(Request())
        
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
        
            
    except Exception as e:
        print(f"❌ Не удалось обновить токен: {e}")

if __name__ == "__main__":
    get_or_create_token()
    # refresh_token()