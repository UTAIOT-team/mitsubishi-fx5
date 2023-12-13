import requests
import logging

# 設定 logging，將訊息寫入 log.txt 檔案
logging.basicConfig(filename='Notifyfaillog.txt', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def lineNotifyMessage(msg):
    try:
        token = "Eq8xqfhI7ePexbm9oHXqXW62mmMURKgIJSY07CrZARn"
        headers = {
            "Authorization": "Bearer " + token, 
            "Content-Type" : "application/x-www-form-urlencoded"
        }

        payload = {'message': msg }
        r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)
        return r.status_code
    except Exception as e:
        logging.error(f"Line Notify failed: {e}")