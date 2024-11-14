from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import time
import selenium
from selenium.webdriver.chrome.service import Service
import pandas as pd
import os

def download_inflation_data(start_date, end_date, download_dir='./scrapping', filename='Data Inflasi.xlsx'):
    # Define the download directory as an absolute path
    download_dir = os.path.abspath(download_dir)

    # Initialize WebDriver with download preferences
    service = Service()
    options = webdriver.ChromeOptions()
    options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,  # Set download directory
        "download.prompt_for_download": False,       # Disable download prompts
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True                 # Disable safe browsing warnings
    })
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Open the specified URL
        url = 'https://www.bi.go.id/id/statistik/indikator/data-inflasi.aspx'
        driver.get(url)

        # Wait until the date input fields are available
        wait = WebDriverWait(driver, 10)

        # Set the start and end dates
        start_date_field = wait.until(EC.element_to_be_clickable((By.ID, "TextBoxDateFrom")))
        start_date_field.clear()
        start_date_field.send_keys(start_date)

        end_date_field = wait.until(EC.element_to_be_clickable((By.ID, "TextBoxDateTo")))
        end_date_field.clear()
        end_date_field.send_keys(end_date)

        # Locate and click the search button
        search_button = driver.find_element(By.XPATH, "//input[@type='submit' and @value='Cari']")
        search_button.click()

        # Wait for the results to load
        time.sleep(5)

        # Try clicking the export button with retry loop and JavaScript click
        for _ in range(3):  # Retry up to 3 times
            try:
                export_button = driver.find_element(By.XPATH, "//input[@type='submit' and @value='Unduh']")
                driver.execute_script("arguments[0].click();", export_button)  # Use JavaScript to click
                time.sleep(2)
                break  # Exit loop if successful
            except selenium.common.exceptions.ElementClickInterceptedException:
                time.sleep(2)

        # Wait for the file to download
        time.sleep(10)  # Adjust time based on download speed

    finally:
        # Close the browser
        driver.quit()

    # Construct the path to the downloaded file
    file_path = os.path.join(download_dir, filename)

    # Check if the file exists, then read it
    if os.path.exists(file_path):
        # Read the Excel file, assuming headers start from the 5th row (index 4)
        df = pd.read_excel(file_path, header=4, usecols="A:C")  # Adjust columns as needed
        return df
    else:
        raise FileNotFoundError(f"The file {filename} was not found in the directory {download_dir}.")
