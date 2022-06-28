import pandas as pd
import sqlalchemy as sql
from sqlalchemy import *
import time
from pathlib import Path
from io import StringIO

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from Scraping.index import *


class Scraper:
    pd.set_option('display.max_columns', 2000)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_rows', 2000)

    def __init__(self):
        self.driver_path = Path('chromedriver.exe')
        self.options = Options()
        self.options.add_argument("--window-size=1920,1080")

        path = Path('data.db')
        db_path = f'sqlite:///{path}'
        self.engine = sql.create_engine(db_path)

    def get_odds_table(self, sport):
        self.options.headless = True
        driver = webdriver.Chrome(options=self.options, executable_path=self.driver_path)
        website = f'https://www.sportsbookreviewsonline.com/scoresoddsarchives/{sport}/{sport}oddsarchives.htm'
        driver.get(website)
        x = 1
        while True:
            try:
                xpath = f'/html/body/table[2]/tbody/tr[1]/td[2]/table/tbody/tr[2]/td/ul/li[{x}]/a'
                # WebDriverWait(driver, 60).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
                d = driver.find_elements_by_xpath(xpath)[0]
                name = d.text
                # print(name)
                data = d.get_attribute('href')
                df = pd.read_excel(data)
                try:
                    if sport == 'NCAABasketball' or 'NBA':
                        name = f'{name[:-8]} {str(int(name[-7:-3]) + 1)}'
                except:
                    pass

                print(name)
                # print(df)
                df.to_sql(f'Raw {name} Odds', self.engine, index=False, if_exists='replace')
                x += 1
            except Exception as e:
                print(e)
                driver.quit()
                break

    def get_bovada_spread(self, sport):
        self.options.headless = True
        driver = webdriver.Chrome(options=self.options, executable_path=self.driver_path)
        if sport == 'MLB':
            sport_url = 'baseball/mlb'
        elif sport == 'NBA':
            sport_url = 'basketball/nba'
        driver.get(f'https://www.bovada.lv/sports/{sport_url}')
        xpath = '/html/body/bx-site/ng-component/div/sp-sports-ui/div/main/div/section/main/sp-path-event/div/div[2]/sp-next-events/div'

        WebDriverWait(driver, 60).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
        x = 1
        new = []
        try:
            while True:
                period = driver.find_element_by_xpath(f'/html/body/bx-site/ng-component/div/sp-sports-ui/div/main/div/section/main/sp-path-event/div/div[2]/sp-next-events/div/div/div[2]/div/sp-coupon[{x}]/sp-multi-markets/section/section/sp-score-coupon/span').text
                if period == 'Second Half':
                    x += 1
                    continue

                away_team_xpath = f'/html/body/bx-site/ng-component/div/sp-sports-ui/div/main/div/section/main/sp-path-event/div/div[2]/sp-next-events/div/div/div[2]/div/sp-coupon[{x}]/sp-multi-markets/section/section/header/sp-competitor-coupon/a/div/h4[1]/span[1]'
                home_team_xpath = f'/html/body/bx-site/ng-component/div/sp-sports-ui/div/main/div/section/main/sp-path-event/div/div[2]/sp-next-events/div/div/div[2]/div/sp-coupon[{x}]/sp-multi-markets/section/section/header/sp-competitor-coupon/a/div/h4[2]/span[1]'
                away_ml_xpath = f'/html/body/bx-site/ng-component/div/sp-sports-ui/div/main/div/section/main/sp-path-event/div/div[2]/sp-next-events/div/div/div[2]/div/sp-coupon[{x}]/sp-multi-markets/section/section/sp-outcomes/sp-two-way-vertical[2]/ul/li[1]/sp-outcome/button/span[1]'
                home_ml_xpath = f'/html/body/bx-site/ng-component/div/sp-sports-ui/div/main/div/section/main/sp-path-event/div/div[2]/sp-next-events/div/div/div[2]/div/sp-coupon[{x}]/sp-multi-markets/section/section/sp-outcomes/sp-two-way-vertical[2]/ul/li[2]/sp-outcome/button/span[1]'
                spread_xpath = f'/html/body/bx-site/ng-component/div/sp-sports-ui/div/main/div/section/main/sp-path-event/div/div[2]/sp-next-events/div/div/div[2]/div/sp-coupon[{x}]/sp-multi-markets/section/section/sp-outcomes/sp-two-way-vertical[1]/ul/li[1]/sp-outcome/button/sp-spread-outcome/span'
                ou_xpath = f'/html/body/bx-site/ng-component/div/sp-sports-ui/div/main/div/section/main/sp-path-event/div/div[2]/sp-next-events/div/div/div[2]/div/sp-coupon[{x}]/sp-multi-markets/section/section/sp-outcomes/sp-two-way-vertical[3]/ul/li[1]/sp-outcome/button/sp-total-outcome/span[2]'

                ml = driver.find_element_by_xpath(away_ml_xpath).text
                away_team = driver.find_element_by_xpath(away_team_xpath).text
                home_team = driver.find_element_by_xpath(home_team_xpath).text
                spread = driver.find_element_by_xpath(spread_xpath).text
                if ml != '':
                    away_ml = driver.find_element_by_xpath(away_ml_xpath).text
                    home_ml = driver.find_element_by_xpath(home_ml_xpath).text
                else:
                    away_ml = 0
                    home_ml = 0
                ou = driver.find_element_by_xpath(ou_xpath).text
                new.append([period, away_team, home_team, spread, away_ml, home_ml, ou])
                x += 1
                time.sleep(1)
        except Exception as e:
            print('')

        cols = ['Date', 'Away_Team', 'Home_Team', 'Spread', 'Away_ML', 'Home_ML', 'Over/Under']
        df = pd.DataFrame(new, columns=cols)
        df.replace('EVEN', 100, inplace=True)
        df = df[df['Away_ML'] != 0]
        df['Spread'] = -df['Spread'].astype(float)
        df['Away_ML'] = df['Away_ML'].astype(float)
        df['Home_ML'] = df['Home_ML'].astype(float)
        df['Over/Under'] = df['Over/Under'].astype(float)
        for index, row in df.iterrows():
            if row['Home_ML'] > 0:
                df.loc[index, 'Home_ML'] = round((row['Home_ML'] / 100) + 1, 2)
            else:
                df.loc[index, 'Home_ML'] = round(1 - (100 / row['Home_ML']), 2)

            if row['Away_ML'] > 0:
                df.loc[index, 'Away_ML'] = round((row['Away_ML'] / 100) + 1, 2)
            else:
                df.loc[index, 'Away_ML'] = round(1 - (100 / row['Away_ML']), 2)

        driver.quit()

        df['Home_Team'] = df['Home_Team'].str.replace(r'\(.*\)', '', regex=True)
        df['Away_Team'] = df['Away_Team'].str.replace(r'\(.*\)', '', regex=True)

        df['Home_Team'] = df['Home_Team'].str.strip()
        df['Away_Team'] = df['Away_Team'].str.strip()

        df = df.replace({'Middle Tenn St': 'Middle Tennessee',
                         'Boston U': 'Boston University',
                         'USC Upstate': 'South Carolina Upstate',
                         'BYU': 'Brigham Young',
                         "St. Peter's": "Saint Peter's",
                         })
        df['Home_Team'] = df['Home_Team'].str.replace(r' ', '')
        df['Away_Team'] = df['Away_Team'].str.replace(r' ', '')

        return df

    def get_betonline_lines(self, sport):
        self.options.headless = False
        driver = webdriver.Chrome(options=self.options, executable_path=self.driver_path)

        soccer = leagues['Soccer']

        url_sport = betonline_url[sport]

        driver.get(f'https://www.betonline.ag/sportsbook/{url_sport}')
        xpath = '/html/body/div[2]/section/app-factory-component/amb-menu-lib/div[2]/div[2]/amb-offering-mainsportsbook/amb-games-lib/div/div[3]/div/div[2]'

        WebDriverWait(driver, 60).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
        y = 1
        new = []
        try:
            while y < 100:
                try:
                    x = 2
                    while True:
                        date = driver.find_element_by_xpath(
                            f'/html/body/div[2]/section/app-factory-component/amb-menu-lib/div[2]/div[2]/amb-offering-mainsportsbook/amb-games-lib/div/div[3]/div/div[2]/div[{y}]/div[{x}]/table/tr/td[1]/a').text
                        away_team = driver.find_element_by_xpath(
                            f'/html/body/div[2]/section/app-factory-component/amb-menu-lib/div[2]/div[2]/amb-offering-mainsportsbook/amb-games-lib/div/div[3]/div/div[2]/div[{y}]/div[{x}]/table/tr/td[2]/lines-row[1]/tr/td[2]/div/a/span').text
                        home_team = driver.find_element_by_xpath(
                            f'/html/body/div[2]/section/app-factory-component/amb-menu-lib/div[2]/div[2]/amb-offering-mainsportsbook/amb-games-lib/div/div[3]/div/div[2]/div[{y}]/div[{x}]/table/tr/td[2]/lines-row[2]/tr/td[2]/div/a/span[1]').text
                        ml_xpath = driver.find_element_by_xpath(
                            f'/html/body/div[2]/section/app-factory-component/amb-menu-lib/div[2]/div[2]/amb-offering-mainsportsbook/amb-games-lib/div/div[3]/div/div[2]/div[{y}]/div[{x}]/table/tr/td[2]/lines-row[1]/tr/td[4]').text
                        try:
                            away_ml = driver.find_element_by_xpath(
                                f'/html/body/div[2]/section/app-factory-component/amb-menu-lib/div[2]/div[2]/amb-offering-mainsportsbook/amb-games-lib/div/div[3]/div/div[2]/div[{y}]/div[{x}]/table/tr/td[2]/lines-row[1]/tr/td[4]/bet-pick/div/div/div').text
                            home_ml = driver.find_element_by_xpath(
                                f'/html/body/div[2]/section/app-factory-component/amb-menu-lib/div[2]/div[2]/amb-offering-mainsportsbook/amb-games-lib/div/div[3]/div/div[2]/div[{y}]/div[{x}]/table/tr/td[2]/lines-row[2]/tr/td[4]/bet-pick/div/div/div').text
                        except Exception as e:
                            away_ml = '-100'
                            home_ml = '-100'

                        try:
                            spread = driver.find_element_by_xpath(
                                f'/html/body/div[2]/section/app-factory-component/amb-menu-lib/div[2]/div[2]/amb-offering-mainsportsbook/amb-games-lib/div/div[3]/div/div[2]/div[{y}]/div[{x}]/table/tr/td[2]/lines-row[1]/tr/td[3]/bet-pick/div/div/div[1]').text
                        except Exception as e:
                            spread = '0'

                        try:
                            ou = driver.find_element_by_xpath(
                                f'/html/body/div[2]/section/app-factory-component/amb-menu-lib/div[2]/div[2]/amb-offering-mainsportsbook/amb-games-lib/div/div[3]/div/div[2]/div[{y}]/div[{x}]/table/tr/td[2]/lines-row[1]/tr/td[5]/bet-pick/div/div/div[1]').text
                        except Exception as e:
                            ou = '0'

                        x += 1

                        time.sleep(1)
                        new.append([date, away_team, home_team, spread, away_ml, home_ml, ou])
                except Exception as e:
                    print(e)
                    pass
                y += 1
        except Exception as e:
            print(e)
            pass

        cols = ['Date', 'Away_Team', 'Home_Team', 'Spread', 'Away_ML', 'Home_ML', 'Over/Under']
        df = pd.DataFrame(new, columns=cols)
        print(df)
        # df.str.replace('EVEN', 100, inplace=True)
        df['Spread'] = df['Spread'].str.replace(r'½', '.5')
        df['Spread'] = df['Spread'].str.replace(r'pk', '0')
        df['Spread'] = df['Spread'].str.rsplit(',', 1).str[1]
        if sport in soccer:
            df['Over/Under'] = df['Over/Under'].str.rsplit(',', 1).str[0]
        df['Over/Under'] = df['Over/Under'].str.replace(r'½', '.5')
        df['Over/Under'] = df['Over/Under'].str.replace(r'O ', '')
        df['Over/Under'] = df['Over/Under'].str.replace(r'U ', '')
        df['Over/Under'] = df['Over/Under'].str.replace(r'O\n', '', regex=True)
        df['Over/Under'] = df['Over/Under'].str.replace(r'U\n', '', regex=True)
        df = df[df['Away_ML'] != 0]
        df['Spread'] = -df['Spread'].astype(float)
        df['Away_ML'] = df['Away_ML'].astype(float)
        df['Home_ML'] = df['Home_ML'].astype(float)
        df['Over/Under'] = df['Over/Under'].astype(float)

        if sport in soccer:
            df['Draw_ML'] = 250

        for index, row in df.iterrows():
            if row['Home_ML'] > 0:
                df.loc[index, 'Home_ML'] = round((row['Home_ML'] / 100) + 1, 2)
            else:
                df.loc[index, 'Home_ML'] = round(1 - (100 / row['Home_ML']), 2)

            if row['Away_ML'] > 0:
                df.loc[index, 'Away_ML'] = round((row['Away_ML'] / 100) + 1, 2)
            else:
                df.loc[index, 'Away_ML'] = round(1 - (100 / row['Away_ML']), 2)

        driver.quit()
        df = df.replace({'NC Wilmington': 'UNC Wilmington', 'NC Asheville': 'UNC Asheville',
                         'No. Colorado': 'Northern Colorado', 'Middle Tennessee State': 'Middle Tennessee',
                         'Fla Gulf Coast': 'Florida Gulf Coast', 'USC Upstate': 'South Carolina Upstate',
                         })
        df.fillna(0, inplace=True)
        return df

    def get_tennis_data(self, season, sport):
        self.options.headless = False
        driver = webdriver.Chrome(options=self.options, executable_path=self.driver_path)
        website_dict = {'WTARome': f'https://github.com/JeffSackmann/tennis_wta/blob/master/wta_matches_{season}.csv',
                        'ATPRome': f'https://github.com/JeffSackmann/tennis_atp/blob/078c51a31a5577798961bc3eed71fa29b8b60bfa/atp_matches_{season}.csv',
                        'ATPGeneva': f'https://github.com/JeffSackmann/tennis_atp/blob/078c51a31a5577798961bc3eed71fa29b8b60bfa/atp_matches_{season}.csv',
                        'ATPLyon': f'https://github.com/JeffSackmann/tennis_atp/blob/078c51a31a5577798961bc3eed71fa29b8b60bfa/atp_matches_{season}.csv',
                        'WTAStrasbourg': f'https://github.com/JeffSackmann/tennis_wta/blob/master/wta_matches_{season}.csv',
                        'WTARabat': f'https://github.com/JeffSackmann/tennis_wta/blob/master/wta_matches_{season}.csv',
                        'ChallengerShymkent2': f'https://github.com/JeffSackmann/tennis_atp/blob/5599f84a9cb34ff01ccb1c6d97631debf878da2b/atp_matches_qual_chall_{season}.csv',
                        'ATPFrenchOpen': f'https://github.com/JeffSackmann/tennis_atp/blob/078c51a31a5577798961bc3eed71fa29b8b60bfa/atp_matches_{season}.csv',
                        }

        xpath = '//*[@id="repo-content-pjax-container"]/div/div/div[4]/div[3]/div/table'
        xpath_2 = '//*[@id="repo-content-pjax-container"]/div/div/div[4]/div[2]/div[2]/table'
        driver.get(website_dict[sport])
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
            elements = StringIO(driver.find_elements_by_xpath(xpath)[0].text)
            element_df = pd.read_csv(elements, sep=",")
        except Exception as e:
            WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, xpath_2)))
            elements = driver.find_elements_by_xpath(xpath_2)[0].get_attribute('outerHTML')
            element_df = pd.read_html(elements)[0]
        driver.quit()

        element_df.to_sql(f'{season} {sport} Raw', self.engine, index=False, if_exists='replace')

    def get_sr_stats(self, sport, season, **kwargs):
        stats = kwargs.get('stats', True)
        self.options.headless = False
        driver = webdriver.Chrome(options=self.options, executable_path=self.driver_path)

        xpaths_dict = {'MLB':
                           {'stats_xpaths': ['//*[@id="teams_standard_batting"]', '//*[@id="teams_standard_pitching"]'],
                            'stats_website': f'https://www.baseball-reference.com/leagues/majors/{season}.shtml',
                            'games_xpath': '',
                            'games_website': ''
                            },
                       'NBA':
                           {'stats_xpaths': ['//*[@id="totals-team"]'],
                            'stats_website': f'https://www.basketball-reference.com/leagues/NBA_{season}.html',
                            'games_xpath': '',
                            'games_website': ''
                            },
                       'NCAABasketball':
                           {'stats_xpaths': ['//*[@id="basic_school_stats"]'],
                            'stats_website': f'https://www.sports-reference.com/cbb/seasons/{season}-school-stats.html',
                            'games_xpath': '',
                            'games_website': ''
                            },
                       'NFL':
                           {'stats_xpaths': ['//*[@id="passing"]', '//*[@id="rushing"]'],
                            'stats_website': f'https://www.pro-football-reference.com/years/{season}/',
                            'games_xpath': '',
                            'games_website': ''
                            },
                       'NHL':
                           {'stats_xpaths': ['//*[@id="stats"]'],
                            'stats_website': f'https://www.hockey-reference.com/leagues/NHL_{season}.html',
                            'games_xpath': '',
                            'games_website': ''
                            },
                       'GreekBasketball':
                           {'stats_xpaths': ['//*[@id="team_stats_totals"]'],
                            'stats_website': f'https://www.basketball-reference.com/international/greek-basket-league/{season}.html',
                            'games_xpath': ['//*[@id="games"]'],
                            'games_website': f'https://www.basketball-reference.com/international/greek-basket-league/{season}-schedule.html'
                            },
                       'EuroLeague':
                           {'stats_xpaths': ['//*[@id="team_stats_totals"]'],
                            'stats_website': f'https://www.basketball-reference.com/international/euroleague/{season}.html',
                            'games_xpath': ['//*[@id="games"]'],
                            'games_website': f'https://www.basketball-reference.com/international/euroleague/{season}-schedule.html'
                            },
                       'EuroCup':
                           {'stats_xpaths': ['//*[@id="team_stats_totals"]'],
                            'stats_website': f'https://www.basketball-reference.com/international/eurocup/{season}.html',
                            'games_xpath': ['//*[@id="games"]'],
                            'games_website': f'https://www.basketball-reference.com/international/eurocup/{season}-schedule.html'
                            },
                       'SpainACB':
                           {'stats_xpaths': ['//*[@id="team_stats_totals"]'],
                            'stats_website': f'https://www.basketball-reference.com/international/spain-liga-acb/{season}.html',
                            'games_xpath': ['//*[@id="games"]'],
                            'games_website': f'https://www.basketball-reference.com/international/spain-liga-acb/{season}-schedule.html'
                            },
                       'VTBUnited':
                           {'stats_xpaths': ['//*[@id="team_stats_totals"]'],
                            'stats_website': f'https://www.basketball-reference.com/international/vtb-united/{season}.html',
                            'games_xpath': ['//*[@id="games"]'],
                            'games_website': f'https://www.basketball-reference.com/international/vtb-united/{season}-schedule.html'
                            },
                       'FranceLNB':
                           {'stats_xpaths': ['//*[@id="team_stats_totals"]'],
                            'stats_website': f'https://www.basketball-reference.com/international/france-lnb-pro-a/{season}.html',
                            'games_xpath': ['//*[@id="games"]'],
                            'games_website': f'https://www.basketball-reference.com/international/france-lnb-pro-a/{season}-schedule.html'
                            },
                       'NBLAustralia':
                           {'stats_xpaths': ['//*[@id="team_stats_totals"]'],
                            'stats_website': f'https://www.basketball-reference.com/international/nbl-australia/{season}.html',
                            'games_xpath': ['//*[@id="games"]'],
                            'games_website': f'https://www.basketball-reference.com/international/nbl-australia/{season}-schedule.html'
                            },
                       'ItalyLega':
                           {'stats_xpaths': ['//*[@id="team_stats_totals"]'],
                            'stats_website': f'https://www.basketball-reference.com/international/italy-basket-serie-a/{season}.html',
                            'games_xpath': ['//*[@id="games"]'],
                            'games_website': f'https://www.basketball-reference.com/international/italy-basket-serie-a/{season}-schedule.html'
                            },
                       'MexicanLMB':
                           {'stats_xpaths': ['//*[@id="team_stats_totals"]'],
                            'stats_website': f'https://www.baseball-reference.com/register/league.cgi?id=73c8b264',
                            'games_xpath': ['//*[@id="games"]'],
                            'games_website': f'https://www.basketball-reference.com/international/eurocup/{season}-schedule.html'
                            },
                       'PremierLeague':
                           {'stats_xpaths': ['//*[@id="stats_squads_standard_for"]'],
                            'stats_website': f'https://fbref.com/en/comps/9/1631/2017-2018-Premier-League-Stats#site_menu_link',
                            'games_xpath': ['//*[@id="games"]'],
                            'games_website': f'https://www.basketball-reference.com/international/eurocup/{season}-schedule.html'
                            },
                       'WNBA':
                           {'stats_xpaths': ['//*[@id="totals-team"]'],
                            'stats_website': f'https://www.basketball-reference.com/wnba/years/{season}.html',
                            'games_xpath': '',
                            'games_website': ''
                            },
                       }
        if stats is True:
            xpaths = xpaths_dict[sport]['stats_xpaths']
            website = xpaths_dict[sport]['stats_website']
        else:
            xpaths = xpaths_dict[sport]['games_xpath']
            website = xpaths_dict[sport]['games_website']
        for xpath in xpaths:
            try:
                try:
                    driver.get(website)
                    WebDriverWait(driver, 60).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
                except:
                    driver.get(website)
                    time.sleep(3)
                    ele = driver.find_element_by_xpath(xpath)
                    driver.execute_script("arguments[0].scrollIntoView();", ele)

                time.sleep(1)
                elements = driver.find_elements_by_xpath(xpath)[0].get_attribute('outerHTML')
                elements_id = driver.find_elements_by_xpath(xpath)[0].get_attribute('id')

            except Exception as e:
                print(e)
                break

            element_df = pd.read_html(elements)[0]
            try:
                element_df.columns = element_df.columns.droplevel(0)
            except:
                pass

            try:
                elements_id = elements_id.replace("_", " ").replace("-", " ").title()
                element_df.to_sql(f'{season} {sport} {elements_id}', self.engine, index=False, if_exists='replace')
            except:
                metadata_obj = MetaData()
                elements_id = elements_id.lower()
                table_drop = Table(f'{season} {sport} {elements_id}', metadata_obj)
                table_drop.drop(self.engine)
                elements_id = elements_id.replace("_", " ").replace("-", " ").title()
                element_df.to_sql(f'{season} {sport} {elements_id}', self.engine, index=False, if_exists='replace')

        driver.quit()

    def odds_portal_scrape(self, sport, season):
        self.options.headless = False
        driver = webdriver.Chrome(options=self.options, executable_path=self.driver_path)
        page = 1
        data = []

        spread_season = ['EPL', 'EuroLeague', 'GermanyBBL', 'ItalyLega', 'VTBUnited', 'SuperLig', 'SuperLeague', 'LaLiga',
                         'SerieA', 'Ligue1', 'Bundesliga']
        season_conv = {'2021': '2020-2021', '2020': '2019-2020', '2019': '2018-2019', '2018': '2017-2018',
                       '2017': '2017-2018', '2016': '2016-2017', '2015': '2015-2016'}

        while True:
            if season != '2022':
                if sport in spread_season:
                    website = f'https://www.oddsportal.com/{oddsportal_urls[sport]}-{season_conv[season]}/results/#/page/{page}/'
                else:
                    website = f'https://www.oddsportal.com/{oddsportal_urls[sport]}-{season}/results/#/page/{page}/'
            else:
                website = f'https://www.oddsportal.com/{oddsportal_urls[sport]}/results/#/page/{page}/'
            table_xpath = '//*[@id="tournamentTable"]'
            try:
                driver.get(website)
                WebDriverWait(driver, 60).until(EC.presence_of_all_elements_located((By.XPATH, table_xpath)))
            except Exception as e:
                print(e)
                return
                # df = pd.DataFrame()
                # df.to_sql(f'Raw {season} {sport} Oddsportal', self.engine, index=False, if_exists='replace')

            elements = driver.find_elements_by_xpath(table_xpath)[0].get_attribute('outerHTML')
            element_df = pd.read_html(elements)[0]

            try:
                element_df.dropna(axis=0, inplace=True)
                element_df = element_df.droplevel(0, axis=1)
                element_df = element_df[element_df["B's"] != "B's"]

            except Exception as e:
                print(e)
                break

            for row in element_df.values.tolist():
                data.append(row)

            page += 1
            time.sleep(2)
        driver.quit()

        df = pd.DataFrame(data)
        df.dropna(axis=0, inplace=True)
        df.drop_duplicates(inplace=True, ignore_index=True)
        # print(df)

        df.to_sql(f'Raw {season} {sport} Oddsportal', self.engine, index=False, if_exists='replace')

    def multi_page_sr(self, sport, season, **kwargs):
        self.options.headless = False
        driver = webdriver.Chrome(options=self.options, executable_path=self.driver_path)

        xpaths = sr_multi_page[sport]['xpaths']
        website = sr_multi_page[sport]['urls'][season]

        for xpath in xpaths:
            try:
                try:
                    driver.get(website)
                    WebDriverWait(driver, 60).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
                except:
                    driver.get(website)
                    time.sleep(3)
                    ele = driver.find_element_by_xpath(xpath)
                    driver.execute_script("arguments[0].scrollIntoView();", ele)

                time.sleep(1)
                elements = driver.find_elements_by_xpath(xpath)[0].get_attribute('outerHTML')
                elements_id = driver.find_elements_by_xpath(xpath)[0].get_attribute('id')

            except Exception as e:
                print(e)
                break

            element_df = pd.read_html(elements)[0]
            try:
                element_df.columns = element_df.columns.droplevel(0)
            except:
                pass

            try:
                elements_id = elements_id.replace("_", " ").replace("-", " ").title()
                element_df.to_sql(f'{season} {sport} {elements_id}', self.engine, index=False, if_exists='replace')
            except:
                metadata_obj = MetaData()
                elements_id = elements_id.lower()
                table_drop = Table(f'{season} {sport} {elements_id}', metadata_obj)
                table_drop.drop(self.engine)
                elements_id = elements_id.replace("_", " ").replace("-", " ").title()
                element_df.to_sql(f'{season} {sport} {elements_id}', self.engine, index=False, if_exists='replace')

            # print(elements_id)
            # print(element_df)

        driver.quit()

    def mulitpage_odds_portal(self, sport, season, **kwargs):
        self.options.headless = False
        driver = webdriver.Chrome(options=self.options, executable_path=self.driver_path)
        page = 1
        data = []

        while True:
            website = f'{odds_portal_multipage[sport][season]}/results/#/page/{page}/'
            table_xpath = '//*[@id="tournamentTable"]'
            try:
                driver.get(website)
                WebDriverWait(driver, 60).until(EC.presence_of_all_elements_located((By.XPATH, table_xpath)))
            except Exception as e:
                print(e)
                return
                # df = pd.DataFrame()
                # df.to_sql(f'Raw {season} {sport} Oddsportal', self.engine, index=False, if_exists='replace')

            elements = driver.find_elements_by_xpath(table_xpath)[0].get_attribute('outerHTML')
            element_df = pd.read_html(elements)[0]

            try:
                element_df.dropna(axis=0, inplace=True)
                element_df = element_df.droplevel(0, axis=1)
                element_df = element_df[element_df["B's"] != "B's"]

            except Exception as e:
                print(e)
                break

            for row in element_df.values.tolist():
                data.append(row)

            page += 1
            time.sleep(2)
        driver.quit()

        df = pd.DataFrame(data)
        df.dropna(axis=0, inplace=True)
        df.drop_duplicates(inplace=True, ignore_index=True)
        # print(df)

        df.to_sql(f'Raw {season} {sport} Oddsportal', self.engine, index=False, if_exists='replace')
