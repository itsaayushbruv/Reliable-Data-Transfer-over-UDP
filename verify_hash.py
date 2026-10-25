import hashlib
import sys

hashes = []

for name in sys.argv[1:]:
    with open(name, "rb") as f:
        h = hashlib.sha256(f.read()).hexdigest()
        hashes.append(h)
        print(name, h)

print("MATCH:", hashes[0] == hashes[1])
print("Files are identical." if hashes[0] == hashes[1] else "Files are different.")
