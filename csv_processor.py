"""
Name :        csv_processor
Author :      Alvaro Zaragoza
Last Update : July, 04, 2023
Description : Read CSV files, calculate votes, and save results in a new CSV File 
"""

import logging
import sys
import platform
from datetime import datetime
import pandas

# Create a looger function
def my_custom_logger(logger_name, level=logging.DEBUG):
    """
    Method to return a custom logger with the given name and level
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    format_string = "%(asctime)s — line: %(lineno)d — %(message)s"
    log_format = logging.Formatter(format_string)
    # Creating and adding the console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)
    # Creating and adding the file handler
    file_handler = logging.FileHandler(logger_name, mode='a')
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)
    return logger

# Create main function
def main():
    # Folder separator accordind to OS
    folder_sep = "\\" if platform.system() == "Windows" else "/"

    # Get current time as string
    now = datetime.now()
    current_time = now.strftime("%m_%d_%Y-%H_%M_%S")

    # # Create new log file
    logger = my_custom_logger(f'log{folder_sep}CSV_Processor_Log_{current_time}.log')

    logger.info("Starting Quorum CSV Processor (version: 1.01)")
    logger.info("---------------------------------------------")

    # Read CSV files into Pandas DataFrames
    logger.info("Reading CSV files...")

    try:
        df_legislators = pandas.read_csv(f'inbox{folder_sep}legislators.csv')
    except:
        logger.info(f'File inbox{folder_sep}legislators.csv not found. App cannot continue.')
        exit()

    try:
        df_bills = pandas.read_csv(f'inbox{folder_sep}bills.csv')
    except:
        logger.info(f'File inbox{folder_sep}bills.csv not found. App cannot continue.')
        exit()

    try:
        df_votes = pandas.read_csv(f'inbox{folder_sep}votes.csv')
    except:
        logger.info(f'File inbox{folder_sep}votes.csv not found. App cannot continue.')
        exit()

    try:
        df_vote_results = pandas.read_csv(f'inbox{folder_sep}vote_results.csv')
    except:
        logger.info(f'File inbox{folder_sep}vote_results.csv not found. App cannot continue.')
        exit()

    print(df_legislators)
    print(df_bills)
    print(df_votes)
    print(df_vote_results)

# Analyze DataFrames
# Calculate
# Create Result CSV file

if __name__ == "__main__":
    main()








