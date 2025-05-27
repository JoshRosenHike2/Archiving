# 🧹 ThoughtSpot Metadata Archiving Script

This script identifies and exports ThoughtSpot models (logical tables) that are:
- Older than a specified number of days
- Have total impressions under a threshold
- Do **not** have alerts attached to their dependent objects

It also previews sample sharing permissions and exports a single hardcoded model by GUID.

---

## ✅ How to Run

python Scripts/archiving_final.py --days 1 --lookback-days 1000 --imp-threshold 10000000 --env-file Scripts/.env
