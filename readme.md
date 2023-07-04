This application will read CVS files, process them, and save result CSV files.
The files bills.csv, legislators.csv, votes.csv, and vote_results.csv 
must exist in the inbox subfolder.
In case of missing CSVs or wrong structure, the app will stop and log an error 
message.
In case of missing data in any row read, the entire row will ingnored but the app
will continue running.
The result CSV files, legislators-support-oppose-count.csv and bills.csv, will be 
saved in the outbox subfolder.
Everytime the app runs a log file will be created in the log subfolder.

This application was written and tested in Python 3.8.5 and uses the Pandas Library.


