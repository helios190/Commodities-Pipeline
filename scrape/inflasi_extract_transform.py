import requests
import pandas as pd
from bs4 import BeautifulSoup
import html

def fetch_inflasi():
    url = "https://webapi.bps.go.id/v1/api/view/domain/0000/model/statictable/lang/ind/id/915/key/c943422ea45ef90be804642156963e2c"

    # Send a GET request to the URL
    response = requests.get(url)

    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        html_content = data.get('data', {}).get('table', '')  # Adjust the key based on your JSON structure

        if html_content:
            # Unescape HTML content
            unescaped_html = html.unescape(html_content)
            soup = BeautifulSoup(unescaped_html, "html.parser")

            # Extract headers
            headers_row = soup.find("tr", class_="xl6311696")  # Adjust the class for the header row
            if headers_row:
                headers = [header.get_text(strip=True) for header in headers_row.find_all("td")]

            # Extract table rows
            data_rows = soup.find_all("tr")[3:]  # Skipping header rows
            data = []

            for row in data_rows:
                cells = row.find_all("td")
                row_data = [cell.get_text(strip=True) for cell in cells]
                # Only add rows with the correct number of columns
                if len(row_data) == len(headers):
                    data.append(row_data)

            # Create a DataFrame
            df = pd.DataFrame(data, columns=headers)
            return df
        else:
            print("No table content found in the response.")
    else:
        print("Failed to fetch data. Status code:", response.status_code)

