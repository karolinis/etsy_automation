import os
import re
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas._libs.tslibs.base
import openpyxl
import sys

def gspread_access():
    gc = gspread.service_account(filename='service_account.json')
    gs = gc.open('Order tracking')
    return gs

def error_handling(number, flag):
    # Used to split the number in the comprehension
    if flag == 0: #tracking number
        try:
            number = number.split('\n')[1]
        except:
            pass
        return str(number) + 'A'
    elif flag == 1: #days in transport string
        try:
            number = number.split('\n')[2]
        except:
            number = '' #the empty string is used to fill the dataframe as we need to have the same number of rows
        return number
    elif flag == 2: #days in transport
        try:
            regex = re.compile(r'\d+')
            number = regex.search(number).group(0)
        except:
            number = ''
        return number
    elif flag == 3: #find company
        try:
            regex = re.compile(r'Last mile=> (.*?), number')
            number = regex.search(number).group(1)
        except:
            number = ''
        return number

def split_string(string):
    # Used to split the string in the comprehension
    try:
        string = string.split(' ')[1]
    except:
        string = ''
    return string

def prepare_dataframe(file):
    # Read the selected XLSX file into a DataFrame
    df = pd.read_excel(file)

    # All columns to lower
    df.columns = [x.lower() for x in df.columns]

    # Strip whitespace from all columns
    df.columns = [x.strip() for x in df.columns]

    # Validate that the DataFrame contains the "order number" and "tracking" columns
    if 'order number' not in df.columns or 'tracking' not in df.columns:
        print('Columns present', df.columns)
        raise ValueError('The selected XLSX file does not contain the required columns.')

    # Remove duplicates
    df_no_duplicates = df.drop_duplicates(subset=['order number', 'tracking'], keep='first')

    print('Duplicates dropped: ' + str(len(df) - len(df_no_duplicates)))

    return df_no_duplicates

def click_button(driver):
    # Time to click the search button, the website often crashes here
    MAX_RETRIES = 10  # maximum number of retries

    # try to find and click the element, retrying if necessary
    for i in range(MAX_RETRIES):
        try:
            wait = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//a/img[@src='/static/img/queren.png']")))
            driver.find_element_by_xpath("//a/img[@src='/static/img/queren.png']").click()
            wait = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='cx_xx']")))
            break  # exit the loop if the operation succeeds
        except Exception:
            if i == MAX_RETRIES - 1:  # reached the maximum number of retries
                raise Exception('Failed to find and click the element after {} retries'.format(MAX_RETRIES))

def slice_dataframe(df):
    # Set the number of rows in each slice to 30
    slice_length = 29

    # Initialize the list of slices
    slices = []

    # Loop through the rows in the dataframe
    for i in range(0, len(df), slice_length):
        # Get the slice of rows from the dataframe
        slice = df.iloc[i:i + slice_length]

        # Add the slice to the list of slices
        slices.append(slice)

    # Return the list of slices
    return slices

def scrape_data_table(driver):

    # Use XPath to find the delivery status
    delivery_status = driver.find_elements_by_xpath('//div[@class="cx_xx"]')
    delivery_status = [status.text for status in delivery_status]
    company = [error_handling(number, 3) for number in delivery_status]

    order_nr = driver.find_elements_by_xpath("//div[@class ='cx_bt_xx']")
    order_nr = [number.text for number in order_nr]
    tracking_nr = [error_handling(number, 0) for number in order_nr] #need to add error handling as not every value has the possibility to be splitted
    days_in_transport = [error_handling(number, 1) for number in order_nr] #need to add error handling as not every value has the possibility to be splitted
    days_in_transport = [error_handling(number, 2) for number in days_in_transport]

    order_nr = [number.split('\n')[0] for number in order_nr]

    return delivery_status, order_nr, tracking_nr, days_in_transport, company

def loop_through_series(df, driver):

    # wait for the placeholder element to load
    wait = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '(//input)[1]')))

    # Use XPath to find the input placeholder
    input_placeholder = driver.find_element_by_xpath('(//input)[1]')

    try: #first time we do not need to click, let's hope for an error
        # Clear the input placeholder for safety
        clear = driver.find_element_by_xpath("//i[@class='icon bxweb bx-guanbi clear_icon']").click()
    except:
        pass

    # Call the function and pass the slice as the argument
    for tracking_nr in df['tracking']:
        input_placeholder.send_keys(str(tracking_nr) + ' ')

    click_button(driver)

    if driver.find_elements_by_xpath("//span[contains(.,'请求超时，请重试')]"):
        click_button(driver)

    # wait for the presence of the target element
    wait = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//div[@class="cx_xx"]')))

    delivery_status, order_nr, tracking_nr, days_in_transport, company = scrape_data_table(driver)

    return delivery_status, order_nr, tracking_nr, days_in_transport, company

def remove_empty_string(list):
    while("" in list):
        list.remove("")
    return list

def loop_through_slices(slices, driver):
    # Create main dataframe
    main_df = pd.DataFrame()

    delivery_statuses = {}
    tracking_nrs = {}
    days = {}
    companies = {}
    # Initiate scraping
    for slice in slices:
        main_df = pd.concat([main_df, slice], axis=0)

        delivery_status, order_nr, tracking_nr, days_in_transport, company = loop_through_series(slice, driver)
        order_nr = [x for x in order_nr if len(x) > 0]  # removes all empty strings

        delivery_statuses.update(dict(zip(order_nr, remove_empty_string(delivery_status))))
        tracking_nrs.update(dict(zip(order_nr, remove_empty_string(tracking_nr))))
        days.update(dict(zip(order_nr, days_in_transport)))
        companies.update(dict(zip(order_nr, company)))

    # Loop through the main dataframe and add the delivery status where key matches tracking
    main_df['delivery_status'] = main_df['tracking'].map(delivery_statuses)
    main_df['tracking_number'] = main_df['tracking'].map(tracking_nrs)
    main_df['days_in_transport'] = main_df['tracking'].map(days)
    main_df['delivery company'] = main_df['tracking'].map(companies)

    return main_df

def days_since(date_str: str) -> int:
    if date_str == '':
        return ''

    # Parse the input date string and convert it to a datetime object
    date = datetime.strptime(date_str, '%Y-%m-%d')

    # Get the current date and time
    now = datetime.now()

    # Calculate the difference between the input date and the current date,
    # expressed in days
    diff = now - date
    return diff.days

def push_to_sheets(df, sheet_name, worksheet_name):
    # Authenticate with Google Sheets API
    gc = gspread.service_account()

    # Open the specified sheet and worksheet
    worksheet = gc.open(sheet_name).worksheet(worksheet_name)

    # Get the current data from the worksheet as a dataframe
    existing_df = pd.DataFrame(worksheet.get_all_records())

    concat_df = pd.concat([df, existing_df], axis=0, join='outer')

    # Remove duplicates from the concatenated dataframe
    concat_df = concat_df.drop_duplicates(subset=['tracking'])

    # Update the worksheet with the deduped dataframe
    set_with_dataframe(worksheet, concat_df)

def main(filename, flag):
    # Feedback to the user
    print('Running file: ', filename)

    if flag == 0:
        df = prepare_dataframe(filename)
    else:
        df = filename

    # Slice the dataframe into slices of 30 rows each
    slices = slice_dataframe(df)

    # Driver option -headless
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # Create a new instance of the Chrome driver
    driver = webdriver.Chrome(options=options)

    # Open the specified website
    driver.get('https://track.yw56.com.cn/cn/querydel')

    main_df = loop_through_slices(slices, driver)

    if flag == 0:
        main_df['File'] = filename

    # Rearrange the columns
    column_order = ["File", "order number", "delivery company", "tracking_number", "tracking", "delivery_status",
                    "days_in_transport"]

    try:
        main_df["order number"] = main_df["order number"].apply(lambda x: x.replace("US", ""))
    except:
        pass

    # reorder the columns of the DataFrame
    main_df = main_df.reindex(columns=column_order)

    # Create backlog with 2 conditions
    backlog_df = main_df[main_df['delivery_status'] == '没有查到物流信息']
    backlog2 = main_df[main_df['delivery_status'].isnull()]
    backlog_df = pd.concat([backlog_df, backlog2], axis=0)

    # Good df is what's left from the main df - backlog
    good_df = main_df[~main_df['tracking'].isin(backlog_df['tracking'])]

    push_to_sheets(good_df, 'Order tracking', 'Good Tracking')
    push_to_sheets(backlog_df, 'Order tracking', 'Backlog')

    # Close the browser
    driver.close()

    print('Done!', filename)

if __name__ == '__main__':
    # Get the current folder
    current_folder = os.getcwd()

    # Get the list of XLSX files in the current folder
    xlsx_files = [f for f in os.listdir(current_folder) if f.endswith('.xlsx')]

    # Ask the user which XLSX file to use for scraping
    print('Which XLSX file do you want to use for scraping? Type 0 for ALL files.')

    print('0 ALL files\n1 Good Tracking\n2 Backlog')
    [print(x+3, y) for x, y in enumerate(xlsx_files)]

    user_choice = int(input('Enter the number of the file: '))

    if user_choice == 0:
        for file in xlsx_files:
            main(file, 0)
    elif user_choice == 1:
        # Authenticate with Google Sheets API
        gc = gspread.service_account()

        # Open the specified sheet and worksheet
        worksheet = gc.open('Order tracking').worksheet('Good Tracking')

        # Get the current data from the worksheet as a dataframe
        existing_df = pd.DataFrame(worksheet.get_all_records())

        main(existing_df, 1)

    elif user_choice == 2:
        # Authenticate with Google Sheets API
        gc = gspread.service_account()

        # Open the specified sheet and worksheet
        worksheet = gc.open('Order tracking').worksheet('Backlog')

        # Get the current data from the worksheet as a dataframe
        existing_df = pd.DataFrame(worksheet.get_all_records())

        main(existing_df, 1)
    else:
        main(xlsx_files[user_choice-3], 0)
