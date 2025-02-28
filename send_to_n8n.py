import requests
import json
import logging

# Configure logging for send_to_n8n.py
logger = logging.getLogger("send_to_n8n")
logger.setLevel(logging.INFO)

# File handler for send_to_n8n.log
fh = logging.FileHandler('send_to_n8n.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Stream handler for console output
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)
logger.addHandler(sh)

# n8n webhook URL
N8N_WEBHOOK_URL = "http://localhost:5678/webhook/pythonindicators"

def send_to_n8n(file_path="ai_data.json"):
    try:
        # Read the organized JSON data
        with open(file_path, "r") as f:
            data = json.load(f)
        
        # Send data to n8n via POST request
        response = requests.post(N8N_WEBHOOK_URL, json=data)
        if response.status_code == 200:
            logger.info("Data sent to n8n successfully")
        else:
            logger.warning(f"Failed to send data to n8n: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Error sending data to n8n: {e}")

if __name__ == "__main__":
    send_to_n8n()