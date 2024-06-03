import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from airflow.NBA_dag.config import credentials
import boto3
from io import StringIO,BytesIO



def run_nba_etl():
    service = Service(executable_path="chromedriver.exe")
    driver = webdriver.Chrome(service=service)

# Initialize the master DataFrame before the loop
    df = pd.DataFrame(columns=['Player', 'Salary', 'Year'])

    for yr in range(1990, 2019):
       page_num = str(yr) + '-' + str(yr + 1) + '/'
       url = 'https://hoopshype.com/salaries/players/' + page_num
       driver.get(url)
       players = driver.find_elements(By.XPATH, '//td[@class="name"]')
       salaries = driver.find_elements(By.XPATH, '//td[@class="hh-salaries-sorted"]')
    
       players_list = [player.text for player in players]
       salaries_list = [salary.text for salary in salaries]
    
    # Ensure players_list and salaries_list are the same length
       if len(players_list) != len(salaries_list):
          print(f"Data mismatch for year {yr}: {len(players_list)} players and {len(salaries_list)} salaries")
          continue
    
       data_tuples = list(zip(players_list[1:], salaries_list[1:]))  # list of each player's name and salary paired together
       temp_df = pd.DataFrame(data_tuples, columns=['Player', 'Salary'])  # create DataFrame of each tuple in list
       temp_df['Year'] = yr  # add season beginning year to each DataFrame
       df = pd.concat([df, temp_df], ignore_index=True)  # append to master DataFrame

# Save the master DataFrame to a CSV file
    s3 = boto3.client('s3', aws_access_key_id=credentials["access_key"], aws_secret_access_key=credentials["access_key"])
                #s3.put_object(Bucket=bucket_name, Key='testing_data/converted_data.csv', Body=csv_content)
    response = s3.get_object(Bucket=credentials["bucket_name"], Key="extracted_data/nba.csv")
    csv_object = response['Body'].read()
    existing_data = pd.read_csv(BytesIO(csv_object))
    data = pd.concat([existing_data, df], ignore_index=True)
    data = data.dropna()
    data = data.drop_duplicates()
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=credentials["bucket_name"], Key="extracted_data/nba.csv")
    
    
    driver.close()
