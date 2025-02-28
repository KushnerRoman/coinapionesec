import json
import logging
from datetime import datetime

# Configure logging for organize_data.py
logger = logging.getLogger("organize_data")
logger.setLevel(logging.INFO)

# File handler for organize_data.log
fh = logging.FileHandler('organize_data.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Stream handler for console output
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)
logger.addHandler(sh)

def organize_data(input_file="recent_data.json", output_file="ai_data.json"):
    try:
        # Read the raw data
        with open(input_file, "r") as f:
            data = json.load(f)
        
        # Organize the data with metadata
        organized_data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "data_points": len(data)
            },
            "data": data
        }
        
        # Save to a new JSON file with indentation for readability
        with open(output_file, "w") as f:
            json.dump(organized_data, f, indent=4)
        logger.info(f"Organized {len(data)} data points into {output_file}")
    except Exception as e:
        logger.error(f"Error organizing data: {e}")

if __name__ == "__main__":
    organize_data()