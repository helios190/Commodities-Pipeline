from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.webdriver.chrome.service import Service
import pandas as pd
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException

def scrape_table_data(urls, start_date, end_date):
    # Initialize WebDriver
    service = Service()
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=service, options=options)

    all_data = []  # List to store all data across URLs

    for url in urls:
        print(f"Scraping data from {url}...")
        driver.get(url)

        wait = WebDriverWait(driver, 10)

        # Locate and set the start date
        start_date_field = wait.until(EC.element_to_be_clickable((By.NAME, "txtTglAwal")))
        start_date_field.clear()
        start_date_field.send_keys(start_date)

        # Locate and set the end date
        end_date_field = wait.until(EC.element_to_be_clickable((By.NAME, "txtTglAkhir")))
        end_date_field.clear()
        end_date_field.send_keys(end_date)

        # Submit the form
        submit_button = driver.find_element(By.NAME, "btnTampil")
        submit_button.click()
        time.sleep(5)

        # Set entries per page to 100
        select = Select(driver.find_element(By.NAME, "example_length"))
        select.select_by_value("100")
        time.sleep(5)

        # Get the table column headers
        headers = [header.text for header in driver.find_elements(By.CSS_SELECTOR, "#example thead th")]

        # Get the total number of pages from pagination
        pagination = driver.find_elements(By.CSS_SELECTOR, "#example_paginate .paginate_button")
        total_pages = int(pagination[-2].text) if len(pagination) > 1 else 1

        # Loop through each page to scrape data
        for page in range(1, total_pages + 1):
            print(f"Scraping page {page} of {total_pages} for {url}...")
            table_rows = driver.find_elements(By.CSS_SELECTOR, "#example tbody tr")
            for row in table_rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                row_data = [cell.text for cell in cells]
                all_data.append(row_data)

            # Click "Next" if not on the last page
            if page < total_pages:
                next_button = driver.find_element(By.ID, "example_next")
                
                # Retry click if intercepted
                retry_attempts = 3
                for attempt in range(retry_attempts):
                    try:
                        driver.execute_script("arguments[0].scrollIntoView();", next_button)
                        driver.execute_script("arguments[0].click();", next_button)
                        time.sleep(5)
                        break
                    except ElementClickInterceptedException:
                        if attempt < retry_attempts - 1:
                            print(f"Attempt {attempt + 1} - Retrying click due to interception.")
                            time.sleep(2)
                        else:
                            print("Failed to click 'Next' after multiple attempts.")
                            raise

    # Close the driver
    driver.quit()

    # Create a DataFrame from the data with the headers as columns
    df = pd.DataFrame(all_data, columns=headers)
    return df


