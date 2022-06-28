import time

import numpy as np
import pandas as pd
import sqlalchemy as sql
from dateutil.parser import parse
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

pd.set_option('display.max_columns', 2000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 2000)

db_path = 'sqlite:///historical_bets'
engine = sql.create_engine(db_path)

driver_path = Path('chromedriver.exe')
options = Options()
options.binary_location = "C:/Program Files/Google/Chrome/Application/chrome.exe"


def get_betonline_bets():
    options.headless = False
    driver = webdriver.Chrome(options=options, executable_path=driver_path)
    driver.get('https://www.betonline.ag/my-account/bet-history')

    WebDriverWait(driver, 60).until(EC.presence_of_all_elements_located((By.XPATH, '/html/body/div[1]/div/div[2]/div[1]/div/form/div[1]/div/input')))
    username_field = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/form/div[1]/div/input')
    username_field.send_keys('rdefiantfx@gmail.com')
    password_field = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/form/div[2]/div/input')
    password_field.send_keys('Baseball11!')
    login_button = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/form/button')
    login_button.click()

    xpath = '/html/body/div[2]/div[3]/p/app-factory-component/amb-my-account-sub-navigation-lib/section/div/amb-bet-history-lib/div/div/div[2]'
    WebDriverWait(driver, 60).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
    driver.find_element_by_xpath('/html/body/div[2]/div[3]/p/app-factory-component/amb-my-account-sub-navigation-lib/section/div/amb-bet-history-lib/div/div/amb-bet-history-filters/div[1]/div[3]/button').click()
    driver.find_element_by_xpath('/html/body/div[2]/div[3]/p/app-factory-component/amb-my-account-sub-navigation-lib/section/div/amb-bet-history-lib/div/div/amb-bet-history-filters/div[2]/div[2]/button[3]').click()
    ele = driver.find_element_by_xpath('/html/body/div[2]/div[3]/p/app-factory-component/amb-my-account-sub-navigation-lib/section/div/amb-bet-history-lib/div/div/div[2]')
    for i in range(100):
        driver.execute_script("arguments[0].scrollBy(0, 500)", ele)
        time.sleep(0.5)
    x = 1
    new = []
    try:
        while True:
            ticket_num = driver.find_element_by_xpath(f'/html/body/div[2]/div[3]/p/app-factory-component/amb-my-account-sub-navigation-lib/section/div/amb-bet-history-lib/div/div/div[2]/div[{x}]/div[1]/div[1]').text
            date = driver.find_element_by_xpath(f'/html/body/div[2]/div[3]/p/app-factory-component/amb-my-account-sub-navigation-lib/section/div/amb-bet-history-lib/div/div/div[2]/div[{x}]/div[1]/div[2]').text
            desc = driver.find_element_by_xpath(f'/html/body/div[2]/div[3]/p/app-factory-component/amb-my-account-sub-navigation-lib/section/div/amb-bet-history-lib/div/div/div[2]/div[{x}]/div[1]/div[3]').text
            type = driver.find_element_by_xpath(f'/html/body/div[2]/div[3]/p/app-factory-component/amb-my-account-sub-navigation-lib/section/div/amb-bet-history-lib/div/div/div[2]/div[{x}]/div[1]/div[5]').text
            status = driver.find_element_by_xpath(f'/html/body/div[2]/div[3]/p/app-factory-component/amb-my-account-sub-navigation-lib/section/div/amb-bet-history-lib/div/div/div[2]/div[{x}]/div[1]/div[6]').text
            amount = driver.find_element_by_xpath(f'/html/body/div[2]/div[3]/p/app-factory-component/amb-my-account-sub-navigation-lib/section/div/amb-bet-history-lib/div/div/div[2]/div[{x}]/div[1]/div[7]/div/span[2]').text
            win = driver.find_element_by_xpath(f'/html/body/div[2]/div[3]/p/app-factory-component/amb-my-account-sub-navigation-lib/section/div/amb-bet-history-lib/div/div/div[2]/div[{x}]/div[1]/div[8]').text
            if status != 'Pending':
                new.append([ticket_num, date, desc, type, status, amount, win])
            x += 1
    except Exception as e:
        print(e)

    cols = ['Ticket #', 'Date', 'Desc', 'Type', 'Status', 'Wager ($)', 'Win/Loss ($)']
    df = pd.DataFrame(new, columns=cols)
    df['Wager ($)'] = df['Wager ($)'].replace('[\$]', '', regex=True).astype(float)
    df['Win/Loss ($)'] = np.where(df['Win/Loss ($)'] == '-', -df['Wager ($)'], df['Win/Loss ($)'])
    df['Win/Loss ($)'] = df['Win/Loss ($)'].replace('[\$]', '', regex=True).astype(float)

    df["Date"] = pd.to_datetime(df["Date"])
    df.to_sql(f'Historical_Bets', engine, index=False, if_exists='append')

    t = pd.read_sql_table('Historical_Bets', con=engine)
    t = t.drop_duplicates(subset='Ticket #')
    t = t.sort_values(by="Ticket #")
    t['Total_Wagered ($)'] = t['Wager ($)'].cumsum()
    t['Total_Profit/Loss ($)'] = t['Win/Loss ($)'].cumsum()
    t['Actual EV'] = (t['Total_Profit/Loss ($)'] / t['Total_Wagered ($)']) * 100

    t.to_sql(f'Historical_Bets', engine, index=False, if_exists='replace')

    driver.quit()

    return df


def clean_hist_bets(df):
    print(df)
    spread_bets_df = df[(df['Type'] == 'Spread') | (df['Type'] == 'Spread - FP')]
    spread_wins = spread_bets_df['Status'].value_counts()['Won']
    spread_losses = spread_bets_df['Status'].value_counts()['Lost']
    spread_total = spread_wins + spread_losses

    ml_bets_df = df[df['Type'] == 'Money Line']
    ml_wins = ml_bets_df['Status'].value_counts()['Won']
    ml_losses = ml_bets_df['Status'].value_counts()['Lost']
    ml_total = ml_wins + ml_losses

    spread_win_percentage = round((spread_wins / spread_total * 100), 2)
    ml_win_percentage = round((ml_wins / ml_total * 100), 2)

    print(spread_win_percentage, ml_win_percentage)


get_betonline_bets()
a = pd.read_sql_table('Historical_Bets', con=engine)
clean_hist_bets(a)
