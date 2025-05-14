## Initial Setup: Import neccesary libraries and set up environment + Authentication to TS
from dotenv import load_dotenv

## Action 1: Search metadata API for all models & dependencies

## Action 2: Filter list of models to show only those that have been created > X days and add GUID to List 

## Action 3: Check to see if those models have any real responses in the last X days (Search data API) if they do discard them from the list

## Action 4: For each model GUID Retrieve list of dependents (Might already have this from Action 1)

## Action 5: Check to see if there was any activity on those dependents (Liveboards / Answers) in the last X days If there was discard GUID from List

## Action 6: Check to see if there are alerts set on any dependencies? (Export TML API) If there is Discard GUID from List

## Action 7: For remaining GUIDS, find and store all Ownership and sharing details for each model and dependents 

## Action 8: Export all data to Archive