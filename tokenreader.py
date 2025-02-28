import tiktoken
with open("ai_data.json", "r", encoding="utf-8") as file:
    json_content = file.read() 

tokenizer = tiktoken.get_encoding("cl100k_base")
num_tokens = len(tokenizer.encode(json_content))

print("Number of tokens:", num_tokens)