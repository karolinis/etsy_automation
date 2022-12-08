import os
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

def error_handling(number):
    # Used to split the number in the comprehension
    try:
        number = number.split('\n')[1]
    except:
        pass
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

def click_button():
    # Time to click the search button, the website often crashes here
    MAX_RETRIES = 10  # maximum number of retries

    # try to find and click the element, retrying if necessary
    for i in range(MAX_RETRIES):
        try:
            wait = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, "//a/img[@src='/static/img/queren.png']")))
            driver.find_element_by_xpath("//a/img[@src='/static/img/queren.png']").click()
            time.sleep(1)
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

    order_nr = driver.find_elements_by_xpath("//div[@class ='cx_bt_xx']")
    order_nr = [number.text for number in order_nr]
    tracking_nr = [error_handling(number) for number in order_nr] #need to add error handling as not every value has the possibility to be splitted
    order_nr = [number.split('\n')[0] for number in order_nr]

    #we need to find
    html = driver.page_source
    soup = BeautifulSoup(html, 'lxml')

    div_elements = soup.find_all('div', {'class': 'cx_lb'})

    order_processed = []

    # Loop through the div elements and find the last li element inside the ul element
    for div in div_elements:
        ul_element = div.find('ul')
        if ul_element:
            # Get all li elements inside the ul element
            li_elements = ul_element.find_all('li')
            # If there are any li elements, find the div with a class of cz_r inside the last li element
            if li_elements:
                last_li = li_elements[-1]
                cz_r = last_li.find('div', {'class': 'cz_r'})
                if cz_r is None: #this means that the order doesn't have a status yet
                    order_processed.append('')
                # If the div with a class of cz_r exists, find the p element inside it
                if cz_r:
                    p = cz_r.find('p')
                    # If the p element exists, append its text to the list
                    if p:
                        order_processed.append(p.text)

    # Split order processed values and keep the date value, use days_since function to get the amount of days
    # the package is in transit
    days_in_transport = [days_since(split_string(value)) for value in order_processed]

    return delivery_status, order_nr, tracking_nr, days_in_transport

def loop_through_series(df):

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

    click_button()

    if driver.find_elements_by_xpath("//span[contains(.,'请求超时，请重试')]"):
        click_button()

    # wait for the presence of the target element
    wait = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//div[@class="cx_xx"]')))

    delivery_status, order_nr, tracking_nr, days_in_transport = scrape_data_table(driver)

    return delivery_status, order_nr, tracking_nr, days_in_transport

def remove_empty_string(list):
    while("" in list):
        list.remove("")
    return list

def loop_through_slices(slices):
    # Create main dataframe
    main_df = pd.DataFrame()

    delivery_statuses = {}
    tracking_nrs = {}
    days = {}
    # Initiate scraping
    for slice in slices:
        main_df = pd.concat([main_df, slice], axis=0)

        delivery_status, order_nr, tracking_nr, days_in_transport = loop_through_series(slice)
        order_nr = [x for x in order_nr if len(x) > 0]  # removes all empty strings

        delivery_statuses.update(dict(zip(order_nr, remove_empty_string(delivery_status))))
        tracking_nrs.update(dict(zip(order_nr, remove_empty_string(tracking_nr))))
        days.update(dict(zip(order_nr, days_in_transport)))

    # Loop through the main dataframe and add the delivery status where key matches tracking
    main_df['delivery_status'] = main_df['tracking'].map(delivery_statuses)
    main_df['tracking_number'] = main_df['tracking'].map(tracking_nrs)
    main_df['days_in_transport'] = main_df['tracking'].map(days)


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

    # Concatenate the input dataframe with the existing data
    concat_df = pd.concat([existing_df, df], ignore_index=True)

    # Remove duplicates from the concatenated dataframe
    deduped_df = concat_df.drop_duplicates()

    # Update the worksheet with the deduped dataframe
    set_with_dataframe(worksheet, deduped_df)


if __name__ == '__main__':
    # Get the current folder
    current_folder = os.getcwd()

    # Get the list of XLSX files in the current folder
    xlsx_files = [f for f in os.listdir(current_folder) if f.endswith('.xlsx')]

    # Ask the user which XLSX file to use for scraping
    print('Which XLSX file do you want to use for scraping?')

    [print(x) for x in enumerate(xlsx_files)]

    user_choice = int(input('Enter the number of the file: '))

    # Feedback to the user
    print('Running file: ', xlsx_files[user_choice])

    df = prepare_dataframe(xlsx_files[user_choice])

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

    main_df = loop_through_slices(slices)

    # Pandas options
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', None)

    # Separate the records that are going into backlog
    # Main df to excel
    main_df.to_excel(f'{xlsx_files[user_choice]}_updated.xlsx', index=False)
    main_df['File'] = xlsx_files[user_choice]

    # Rearrange the columns
    cols = main_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    main_df = main_df[cols]

    backlog_df = main_df[main_df['delivery_status'] == '没有查到物流信息']
    backlog2 = main_df[main_df['delivery_status'].isnull()]
    backlog_df = pd.concat([backlog_df, backlog2], axis=0)

    # Good df is what's left from the main df - backlog
    good_df = main_df[~main_df['tracking'].isin(backlog_df['tracking'])]

    push_to_sheets(good_df, 'Order tracking', 'test_good')
    push_to_sheets(backlog_df, 'Order tracking', 'test_backlog')

    # Close the browser
    driver.close()

    if sys.exitcode == 0:
        # If the exit code is 0, the pyinstaller command was successful
        # Use the exit() function to shut down the program
        exit()
    else:
        # If the exit code is not 0, the pyinstaller command failed
        # Print an error message and exit the program
        print("Error: pyinstaller command failed")
        exit(1)