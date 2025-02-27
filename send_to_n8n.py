import requests
import json

# n8n webhook URL
N8N_WEBHOOK_URL = "https://your-n8n-instance.com/webhook/your-webhook-id"

def send_to_n8n(file_path="ai_data.json"):
    # Read the organized JSON data
    with open(file_path, "r") as f:
        data = json.load(f)
    
    # Send data to n8n via POST request
    try:
        response = requests.post(N8N_WEBHOOK_URL, json=data)
        if response.status_code == 200:
            print("Data sent to n8n successfully.")
        else:
            print(f"Failed to send data: {response.status_code}")
    except Exception as e:
        print(f"Error sending data to n8n: {e}")

if __name__ == "__main__":
    send_to_n8n()