# Enable API Access for a Project
# Head to Google Developers Console and create a new project (or select the one you already have).
# In the box labeled “Search for APIs and Services”, search for “Google Drive API” and enable it.
# In the box labeled “Search for APIs and Services”, search for “Google Sheets API” and enable it.

import gspread
gc = gspread.service_account()
sh = gc.open("Example spreadsheet")
print(sh.sheet1.get('A1'))