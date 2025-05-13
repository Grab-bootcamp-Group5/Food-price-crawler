import re

for name in ["Thành phố Đà Nẵng", "TP. Hồ Chí Minh", "Tỉnh Bình Dương"]:
    stripped = re.sub(r"^(Thành phố|TP\.|Tỉnh)\s*", "", name).strip()
    print(f"'{name}' → '{stripped}'")
