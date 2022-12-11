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
import tkinter as tk
from tkinter import filedialog, font
from tkinter import messagebox

def last_mile(driver):
    # we need to find last_mile value from html code
    html = driver.page_source
    soup = BeautifulSoup(html, 'lxml')

    div_elements = soup.find_all('div', {'class': 'cx_lb'})

    tracking_nr = []
    company = []
    has_match = False

    # Loop through the div elements and find the last li element inside the ul element
    for div in div_elements:
        ul_element = div.find('ul')
        if ul_element:
            # Get all li elements inside the ul element
            li_elements = ul_element.find_all('li')
            # If there are any li elements, find the div with a class of cz_r inside the last li element
            if li_elements:
                for li in li_elements:
                    div_cz_r = li.find('div', {'class': 'cz_r'})
                    if div_cz_r:
                        # Get the h6 element inside the div element
                        h6_element = div_cz_r.find('h6')
                        if h6_element:
                            # Check if the h6 element contains a match for the regex expression
                            regex = re.compile('Last mile=> (.*?), number (.*)')
                            match = regex.search(h6_element.text)
                            if match:
                                company_name = match.group(1)
                                tracking_number = match.group(2)
                                # If there is a match, add the captured group to the company and tracking_nr list
                                company.append(company_name)
                                tracking_nr.append(tracking_number + 'A')
                                has_match = True
            # If there was no match for the regex expression, add an empty string to the last_mile list
            if not has_match:
                company.append('')
                tracking_nr.append('')
    return tracking_nr, company

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
    #company = [error_handling(number, 3) for number in delivery_status]

    order_nr = driver.find_elements_by_xpath("//div[@class ='cx_bt_xx']")
    order_nr = [number.text for number in order_nr]
    #tracking_nr = [error_handling(number, 0) for number in order_nr] #need to add error handling as not every value has the possibility to be splitted
    days_in_transport = [error_handling(number, 1) for number in order_nr] #need to add error handling as not every value has the possibility to be splitted
    days_in_transport = [error_handling(number, 2) for number in days_in_transport]

    order_nr = [number.split('\n')[0] for number in order_nr]

    tracking_nr, company = last_mile(driver)

    return delivery_status, order_nr, tracking_nr, days_in_transport, company

def loop_through_series(df, driver):

    # wait for the placeholder element to load
    wait = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '(//input)[1]')))

    # Use XPath to find the input placeholder
    input_placeholder = driver.find_element_by_xpath('(//input)[1]')

    try: #first time we do not need to click, let's hope for an error

        click = driver.find_element_by_xpath("//i[@class='icon bxweb bx-guanbi clear_icon']").click()
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
    last_miles = {}
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
    driver = webdriver.Chrome()

    # Open the specified website
    driver.get('https://track.yw56.com.cn/cn/querydel')

    main_df = loop_through_slices(slices, driver)

    if flag == 0:
        filename = os.path.basename(filename)
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

def welcome():
    # create the main window
    window = tk.Tk()
    window.title("Scrape Ywen")

    # create a text label welcoming the user
    label = tk.Label(text="       Welcome to the Ywen scraper!")
    label.config(font=("Courier", 18))
    label.grid()

    # create a font for the text
    text_font = font.Font(family="Helvetica", size=12)

    # create a variable to store the selected option
    option = tk.StringVar(window, "Options")

    # create a variable to store the selected files
    files = []

    # create a function to handle the file upload
    def upload_files():
        # open a file dialog to allow the user to select multiple files
        files.extend(filedialog.askopenfilenames(filetypes=[("Excel files", "*.xlsx")]))

        # create a table to display the selected files
        file_table = tk.Frame(window)
        file_table.grid(row=5, column=0, padx=10, pady=(0, 10))
        for file in files:
            file_label = tk.Label(file_table, text=file, font=text_font)
            file_label.pack()

    def refresh():
        window.destroy()
        welcome()

    # create a function to run the script with the given options and files
    def run_script(option, files):
        print(option.get())
        # destroy the window
        if option.get() == 'Options' and files != []: #run through selected files
            for file in files:
                main(file, 0)
            messagebox.showinfo("Finished", "The program has finished running.")
        elif option.get() == 'All files' and files == []: #run through all files in the folder
            # Get the current folder
            current_folder = os.getcwd()

            # Get the list of XLSX files in the current folder
            xlsx_files = [f for f in os.listdir(current_folder) if f.endswith('.xlsx')]

            for file in xlsx_files:
                main(file, 0)
            messagebox.showinfo("Finished", "The program has finished running.")
        elif option.get() == 'Good Tracking' and files == []:
            # Authenticate with Google Sheets API
            gc = gspread.service_account()

            # Open the specified sheet and worksheet
            worksheet = gc.open('Order tracking').worksheet('Good Tracking')

            # Get the current data from the worksheet as a dataframe
            existing_df = pd.DataFrame(worksheet.get_all_records())

            main(existing_df, 1)
            messagebox.showinfo("Finished", "The program has finished running.")
        elif option.get() == 'Backlog' and files == []:
            # Authenticate with Google Sheets API
            gc = gspread.service_account()

            # Open the specified sheet and worksheet
            worksheet = gc.open('Order tracking').worksheet('Backlog')

            # Get the current data from the worksheet as a dataframe
            existing_df = pd.DataFrame(worksheet.get_all_records())

            main(existing_df, 1)
            messagebox.showinfo("Finished", "The program has finished running.")
        else:
            # instead of printing, show a message box
            messagebox.showerror("Error", "Please select an option OR upload a file (not both) and press RUN\n'"
                                          "'Try refreshing the program and try again.")


    # create a label to explain the dropdown menu
    option_label = tk.Label(window, text="Please select an option:", font=text_font)
    option_label.grid(row=3, column=0, padx=10, pady=(10, 0), sticky="w")

    # create a dropdown menu with the options
    option_menu = tk.OptionMenu(window, option, "All Files", "Good Tracking", "Backlog")
    option_menu.config(font=text_font)
    option_menu.grid(row=3, column=1, padx=10, pady=(10, 0), sticky="w")

    # create a label to explain the file upload section
    upload_label = tk.Label(window, text="Or select Excel files:", font=text_font)
    upload_label.grid(row=4, column=0, padx=10, pady=(10, 0), sticky="w")

    # create a button to allow the user to upload files
    upload_button = tk.Button(window, text="Upload Files", font=text_font, command=upload_files)
    upload_button.grid(row=4, column=1, padx=10, pady=(10, 0), sticky="w")

    # create a button to run the script
    run_button = tk.Button(window, text="Run", font=text_font, command=lambda: run_script(option, files))
    run_button.grid(row=8, column=1, padx=10, pady=(10, 10), sticky="e")
    run_button.config(bg="green", fg="white", width=10, height=1)

    # create a button to run the script
    refresh_button = tk.Button(window, text="Refresh", font=text_font, command=lambda : refresh())
    refresh_button.grid(row=8, column=0, padx=10, pady=(10, 10), sticky="e")
    refresh_button.config(bg="red", fg="white", width=10, height=1)

    # start the main event loop
    window.mainloop()

if __name__ == '__main__':
    welcome()
