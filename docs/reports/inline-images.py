"""Replace local image references with base64 data URIs."""
import base64
import re
import sys

with open(sys.argv[1]) as f:
    content = f.read()

def to_data_uri(match):
    path = match.group(1)
    try:
        with open(path, "rb") as img:
            data = base64.b64encode(img.read()).decode()
        return f"![diagram](data:image/png;base64,{data})"
    except OSError:
        return match.group(0)

content = re.sub(r"!\[diagram\]\((/[^)]+\.png)\)", to_data_uri, content)
print(content)
