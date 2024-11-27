import requests
import pandas as pd
from bs4 import BeautifulSoup
import html

def inflasi():
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
            data_rows = soup.find_all("tr")[2:]  # Skipping header rows
            data = []

            for row in data_rows:
                cells = row.find_all("td")
                row_data = [cell.get_text(strip=True) for cell in cells]
                # Only add rows with the correct number of columns
                if len(row_data) == len(headers):
                    data.append(row_data)

            # Create a DataFrame
            df = pd.DataFrame(data, columns=headers)

            # Transpose DataFrame and reshape it into two columns
            include_months = ["Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus", "September", "Oktober", "November", "Desember"]
            df_filtered = df[df["Bulan"].isin(include_months)]
            df_transposed = df_filtered.melt(id_vars=["Bulan"], var_name="Year", value_name="Value")
            df_transposed["Year-Date"] = df_transposed["Year"] + "-" + df_transposed["Bulan"]
            
            # Split 'Year-Date' into separate 'Year' and 'Month' columns
            df_transposed[['Year', 'Month']] = df_transposed['Year-Date'].str.split('-', expand=True)
            
            # Drop 'Year-Date' column if no longer needed
            df_transposed = df_transposed.drop(columns=["Year-Date"])
            
            # Mapping from Indonesian months to English months
            month_map = {
                "Januari": "January", "Februari": "February", "Maret": "March", "April": "April",
                "Mei": "May", "Juni": "June", "Juli": "July", "Agustus": "August", "September": "September",
                "Oktober": "October", "November": "November", "Desember": "December"
            }
            
            # Apply the mapping to convert Indonesian month names to English
            df_transposed['Month'] = df_transposed['Month'].map(month_map)

            # Convert 'Year' and 'Month' into a datetime format
            df_transposed['Date'] = pd.to_datetime(df_transposed['Year'] + '-' + df_transposed['Month'] + '-01', format='%Y-%B-%d')

            # Extract 'YYYY-MM' format (Year-MonthNumber)
            df_transposed['Year-Month'] = df_transposed['Date'].dt.strftime('%Y-%m')  # Full Year and Month as number
            
            # Drop the 'Date' column as it is no longer needed
            df_final = df_transposed.drop(columns=["Date"])

            # Clean 'Value' column: Remove commas, replace empty strings with NaN
            df_final['Value'] = df_final['Value'].str.replace(',', '.').replace('', 0)

            # Convert 'Value' to float
            df_final['Value'] = pd.to_numeric(df_final['Value'], errors='coerce')

            # Reorder columns and rename 'Year-Month' to 'Date'
            df_final = df_final[["Year-Month", "Value"]]  # Adjust column order if needed
            df_final = df_final.rename(columns={"Year-Month": "Date"})

            # Print final DataFrame
            print(df_final)
            return df_final
        else:
            print("No table content found in the response.")
    else:
        print("Failed to fetch data. Status code:", response.status_code)

inflasi()
