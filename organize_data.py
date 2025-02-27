import json
from datetime import datetime

def organize_data(input_file="recent_data.json", output_file="ai_data.json"):
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
    print("Data organized and saved to ai_data.json")

if __name__ == "__main__":
    organize_data()