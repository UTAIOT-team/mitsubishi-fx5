import requests

def lineNotifyMessage(msg):
    token = "Eq8xqfhI7ePexbm9oHXqXW62mmMURKgIJSY07CrZARn"
    headers = {
        "Authorization": "Bearer " + token, 
        "Content-Type" : "application/x-www-form-urlencoded"
    }

    payload = {'message': msg }
    r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)
    return r.status_code