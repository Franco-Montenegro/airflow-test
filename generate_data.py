import csv, random
from datetime import datetime, timedelta
from pathlib import Path

OUT_PATH = Path("./data/flights_sample.csv")
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

origins = ["SCL","ANF","IQQ","PMC","CJC","LSC"]
destinations = ["SCL","ANF","IQQ","PMC","CJC","LSC"]
airlines = ["LATAM","Sky","JetSmart"]

random.seed(42)
start_date = datetime(2025, 7, 1)
rows = 50000

with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["flight_date","origin","destination","airline","price","currency"])
    for _ in range(rows):
        d = start_date + timedelta(days=random.randint(0, 45))
        o = random.choice(origins)
        ddd = random.choice([x for x in destinations if x != o])
        airline = random.choice(airlines)
        price = random.randint(15000, 150000)
        w.writerow([d.strftime("%Y-%m-%d"), o, ddd, airline, price, "CLP"])

print(f"CSV generado en {OUT_PATH.resolve()}")