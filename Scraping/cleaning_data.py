import pandas as pd
import sqlalchemy as sql
from pathlib import Path

from Scraping.scraping import Scraper
from Scraping.index import *

pd.set_option('display.max_columns', 2000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 10000)


class CleanData:
    def __init__(self):
        self.scraper = Scraper()
        self.engine = self.scraper.engine

    def clean_sr_stats(self, sport, season, **kwargs):
        rerun = kwargs.get('rerun', False)

        international_basketball_league = ['EuroLeague', 'EuroCup', 'NBLAustralia', 'FranceLNB', 'VTBUnited', 'SpainACB', 'GreekBasketball', 'ItalyLega']
        tennis_github = ['ATPRome', 'WTARome', 'ATPGeneva', 'ATPLyon', 'WTAStrasbourg', 'WTARabat', 'ChallengerShymkent2', 'ATPFrenchOpen']
        clay_tennis = ['ATPRome', 'WTARome', 'ATPGeneva', 'ATPLyon', 'WTAStrasbourg', 'WTARabat', 'ChallengerShymkent2', 'ATPFrenchOpen']
        soccer = leagues['Soccer']
        baseball = leagues['Baseball']

        if rerun is True:
            if sport in tennis_github:
                self.scraper.get_tennis_data(season, sport)
            elif sport in sr_multi_page.keys():
                self.scraper.multi_page_sr(sport, season)
            else:
                self.scraper.get_sr_stats(sport, season)
        if sport in baseball:
            try:
                batting_df = pd.read_sql_table(f'{season} {sport} Teams Standard Batting', con=self.engine)
                pitching_df = pd.read_sql_table(f'{season} {sport} Teams Standard Pitching', con=self.engine)
                batting_df = batting_df.drop(columns=['BatAge', 'R/G', 'G', '#Bat'])
                pitching_df = pitching_df.drop(columns=['PAge', 'W', 'L', 'W-L%', 'G', '#P'])

            except Exception as e:
                batting_df = pd.read_sql_table(f'{season} {sport} League Batting', con=self.engine)
                pitching_df = pd.read_sql_table(f'{season} {sport} League Pitching', con=self.engine)
                batting_df = batting_df.drop(columns=['BatAge', 'R/G', 'G', 'Aff'])
                pitching_df = pitching_df.drop(columns=['PAge', 'W', 'L', 'W-L%', 'Aff', 'SHO'])
            
            try:
                df = pd.merge(pitching_df, batting_df, on='Tm', suffixes=('_Pitching', '_Batting'))
            except:
                df = pd.merge(pitching_df, batting_df, on='Finals', suffixes=('_Pitching', '_Batting'))
                df.rename(columns={'Finals': 'Tm'}, inplace=True)
            df = df.drop(columns=['GS', 'GF'])
            df['Team'] = df['Tm'].replace(baseball_sr_odds_conv[sport])
            df['Tm'] = df['Team'].replace(betonline_baseball_conv)
            df = df[df['Tm'] != 'League Totals']

            df.to_sql(f'{season} {sport} Stats', self.engine, index=False, if_exists='replace')

        elif sport == 'NBA':
            df = pd.read_sql_table(f'{season} NBA Totals Team', con=self.engine)
            df['City'] = df['Team'].str.rsplit(' ', 1).str[0]
            df['Mascot'] = df['Team'].str.rsplit(' ', 1).str[1]
            df['City'] = df['City'].str.replace(' ', '')
            df['Mascot'] = df['Mascot'].str.replace('*', '')
            df['City'] = df['City'].str.replace('LA', 'LAClippers')
            df['City'] = df['City'].str.replace('LosAngeles', 'LALakers')
            df['Tm'] = df['City'] + df['Mascot']
            df['Team'] = df['City']

        elif sport == 'NHL':
            df = pd.read_sql_table(f'{season} NHL Stats', con=self.engine)
            df = df.rename(columns={'Unnamed: 1_level_1': 'Team'})
            df['Team'] = df['Team'].str.replace('*', '')
            df['City'] = df['Team'].str.rsplit(' ', 1).str[0]
            df['Mascot'] = df['Team'].str.rsplit(' ', 1).str[1]
            df['Team'] = df['Team'].str.replace(' ', '')
            df = df.rename(columns={'Team': 'Tm', 'City': 'Team'})

        elif sport in international_basketball_league:
            try:
                df = pd.read_sql_table(f'{season} {sport} Team Stats Totals', con=self.engine)
            except Exception as e:
                self.scraper.get_sr_stats(sport, season, stats=True)
                df = pd.read_sql_table(f'{season} {sport} Team Stats Totals', con=self.engine)

            df['Tm'] = df['Club']
            df['Team'] = df['Club'].str.replace(r'*', '', regex=True).str.replace(r' ', '', regex=True)
            df['Tm'] = df['Tm'].replace({'AX Armani Exchange Milano': 'Olimpia Milano', 'FC Barcelona': 'Barcelona',
                                         'Olympiakos': '', 'Club Joventut Badalona': 'Joventut', 'Valencia Basket': 'Valencia',
                                         'Herbalife Gran Canaria': 'GranCanaria', 'Unicaja': 'UnicajaMalaga', 'Hereda San Pablo Burgos': 'Burgos',
                                         'Monbus Obradoiro': 'Blusens Monbus Obradoiro CAB', 'Fortitudo Kiğılı Bologna': 'VirtusBologna',
                                         'Dolomiti Energia Trento': 'Trento', 'Happy Casa Brindisi': 'EnelBrindisi',
                                         'UnaHotels Reggio Emilia': 'ReggioEmilia', 'Banco di Sardegna Sassari': 'DinamoSassari',
                                         'Openjobmetis Varese': 'Varese', 'Umana Reyer Venezia': 'Venezia', 'NutriBullet Treviso': 'DeLonghiTreviso',
                                         'Allianz Pallacanestro Trieste': 'Trieste', 'Carpegna Prosciutto Basket Pesaro': 'Pesaro',
                                         'GeVi Napoli Basket': 'CuoreNapoli', 'Germani Basket Brescia': 'BresciaLeonessa', 'Bertram Tortona': 'DerthonaBasket',
                                         'ESSM Le Portel': 'LePortel', 'Cholet Basket': 'Cholet', 'JL Bourg': 'JLBourgBasket',
                                         'BCM Gravelines-Dunkerque': 'Gravelines', 'JDA Dijon': 'Dijon', 'Chorale Roanne Basket': 'Roanne',
                                         'Fos Provence': 'FosOuestBasket', 'Nanterre 92': 'Nanterre', 'AS Monaco Basket': 'Monaco',
                                         'Champagne Basket': 'Chalons-Reims', 'Orléans Loiret Basket': 'Orleans', 'Limoges CSP': 'CSPLimoges',
                                         'Paris Basketball': 'ParisBasketball', 'Metropolitans 92': 'LevalloisMetropolitans',
                                         'Élan Béarnais Pau-Lacq-Orthez': 'PAUOrthez', 'Le Mans Sarthe Basket': 'LeMans',
                                         'SIG Strasbourg': 'Strasbourg', 'LDLC ASVEL': 'AsvelLyon', 'Río Breogán': 'BreoganLugo',
                                         'Urbas Fuenlabrada': 'Fuenlabrada', 'Surne Bilbao Basket': 'Bilbao', 'MoraBanc Andorra': 'RiverAndorra',
                                         'Casademont Zaragoza': 'CAIZaragoza', 'Coosur Real Betis': 'RealBetis', 'Apollon Patras Oscar': 'ApollonPatras',
                                         'Promitheas Patras': 'ASPPromitheasPatras', 'Larisa': 'Larissa', 'Olympiacos': 'Olympiakos',
                                         'Virtus Segafredo Bologna': 'VirtusBologna', 'Frutti Extra Bursaspor': 'Bursaspor',
                                         'Baxi Manresa': 'Manresa', 'Bitci Baskonia': 'SaskiBaskonia', 'Lenovo Tenerife': 'CB1939Canarias',
                                         'Panathinaikos OPAP': 'Panathinaikos', 'Kolossos H Hotels': 'KolossosRodos',
                                         'PAOK mateco': 'PAOK', 'Lavrio Megabolt': 'GSLavrio', 'Peristeri Vitabiotics': 'Peristeri',
                                         'Aris': 'ArisThessaloniki'})

        elif sport == 'NCAABasketball':
            df = pd.read_sql_table(f'{season} {sport} Basic School Stats', con=self.engine)
            df = df.drop(['Unnamed: 8_level_1', 'Unnamed: 11_level_1', 'Unnamed: 14_level_1', 'Unnamed: 17_level_1',
                          'Unnamed: 20_level_1'], axis=1)
            df = df.dropna()
            df = df[df['Rk'] != 'Rk']
            df['School'] = df['School'].str.replace(r'NCAA', '', regex=True)
            df = df.drop(columns=['Rk', 'G', 'W', 'L', 'W-L%', 'SRS', 'SOS', 'Tm.', 'Opp.', 'MP'], axis=1)
            df['School'] = df['School'].str.replace(r' ', '')
            df['School'] = df['School'].str.replace(r'(OH)', 'Ohio', regex=True)
            df['School'] = df['School'].str.replace(r'(FL)', 'Florida', regex=True)
            df['School'] = df['School'].str.replace(r'(IL)', 'Illinois', regex=True)
            df['School'] = df['School'].str.replace(r'(MD)', 'Maryland', regex=True)
            df['School'] = df['School'].str.replace(r'(NY)', 'NewYork', regex=True)

            df.rename(columns={'School': 'Team'}, inplace=True)

        elif sport in clay_tennis:
            df = pd.read_sql_table(f'{season} {sport} Raw', con=self.engine)
            df = df[df['surface'] == 'Clay']
            winner_df = df[['winner_name', 'w_ace', 'w_df', 'w_svpt', 'w_1stIn', 'w_1stWon', 'w_2ndWon', 'w_SvGms', 'w_bpSaved', 'w_bpFaced']]
            winner_df.columns = ['Team', 'Ace', 'Df', 'SvPT', '1stIn', '1stWon', '2ndWon', 'SvGms', 'bpSaved', 'bpFaced']
            loser_df = df[['loser_name', 'l_ace', 'l_df', 'l_svpt', 'l_1stIn', 'l_1stWon', 'l_2ndWon', 'l_SvGms', 'l_bpSaved', 'l_bpFaced']]
            loser_df.columns = ['Team', 'Ace', 'Df', 'SvPT', '1stIn', '1stWon', '2ndWon', 'SvGms', 'bpSaved', 'bpFaced']
            final = winner_df.append(loser_df, ignore_index=True)
            df = final.groupby(['Team'], as_index=False).sum()
            df['Team'] = df['Team'].str.rsplit(' ', 1).str[-1]
            df['Tm'] = df['Team']
            print(df)

        elif sport == 'WNBA':
            df = pd.read_sql_table(f'{season} WNBA Totals Team', con=self.engine)
            df['Team'] = df['Team'].str.replace(r'*', '')
            df['Tm'] = df['Team']
            print(df)

        elif sport in soccer:
            standard_df = pd.read_sql_table(f'{season} {sport} Stats Squads Standard For', con=self.engine)
            keeper_df = pd.read_sql_table(f'{season} {sport} Stats Squads Keeper For', con=self.engine)

            df = pd.merge(standard_df, keeper_df, on='Squad', suffixes=('_Standard', '_Keeper'))
            print(df)
            df = df.drop(columns=['# Pl_Standard', 'MP_Standard', 'Starts_Standard', 'Min_Standard', '90s_Standard', '# Pl_Keeper', 'MP_Keeper', 'Starts_Keeper', 'Min_Keeper', '90s_Keeper', 'W', 'D', 'L', 'Age'], axis=1)
            df['Team'] = df['Squad'].replace(soccer_sr_odds_conv[sport])
            df['Tm'] = df['Team'].replace(soccer_sr_betonline_conv[sport])
            df = df.drop(columns=['Squad'])
            df = df.fillna(0)

            print(df)

        else:
            df = pd.DataFrame()

        print(df)
        df.to_sql(f'Cleaned {season} {sport} Stats', self.engine, index=False, if_exists='replace')

        return df

    def clean_odds(self, sport, season, **kwargs):
        rerun = kwargs.get('rerun', False)
        international_basketball_league = ['EuroLeague', 'EuroCup', 'NBLAustralia', 'FranceLNB', 'VTBUnited', 'SpainACB', 'GreekBasketball', 'ItalyLega']

        oddsportal_tennis = ['ATPRome', 'WTARome', 'ATPGeneva', 'ATPLyon', 'WTAStrasbourg', 'WTARabat', 'ChallengerShymkent2', 'ATPFrenchOpen']
        soccer = leagues['Soccer']
        oddsportal_sports = oddsportal_urls.keys()
        mulitpage_oddsportal = odds_portal_multipage.keys()

        if sport in international_basketball_league:
            if rerun is True:
                self.scraper.get_sr_stats(sport, season, stats=False)
            try:
                df = pd.read_sql_table(f'{season} {sport} Games', con=self.engine)
            except:
                self.scraper.get_sr_stats(sport, season, stats=False)
                df = pd.read_sql_table(f'{season} {sport} Games', con=self.engine)

        elif sport in oddsportal_sports:
            if rerun is True:
                self.scraper.odds_portal_scrape(sport, season)
            try:
                df = pd.read_sql_table(f'Raw {season} {sport} Oddsportal', con=self.engine)
            except:
                self.scraper.odds_portal_scrape(sport, season)
                df = pd.read_sql_table(f'Raw {season} {sport} Oddsportal', con=self.engine)

        elif sport in mulitpage_oddsportal:
            if rerun is True:
                self.scraper.mulitpage_odds_portal(sport, season)
            try:
                df = pd.read_sql_table(f'Raw {season} {sport} Oddsportal', con=self.engine)
            except:
                self.scraper.odds_portal_scrape(sport, season)
                df = pd.read_sql_table(f'Raw {season} {sport} Oddsportal', con=self.engine)

        else:
            if rerun is True:
                self.scraper.get_odds_table(sport)
            try:
                df = pd.read_sql_table(f'Raw {sport} {season} Odds', con=self.engine)
            except:
                self.scraper.get_odds_table(sport)
                df = pd.read_sql_table(f'Raw {sport} {season} Odds', con=self.engine)
            df['Close'] = df['Close'].fillna(0)

        df.columns = df.columns.str.replace(' ', '')

        print(df)

        spread = []
        ou = []
        away_ml = []
        home_ml = []
        draw_ml = []
        home_team = []
        away_team = []
        final_home = []
        final_away = []
        away_pitcher = []
        home_pitcher = []

        x = 0

        if sport == 'NCAABasketball':
            df = df.loc[:, :'2H']
            # df = df[:1000]
            df.replace('pk', -1, inplace=True)
            df.replace('PK', -1, inplace=True)
            df.replace('NL', -1, inplace=True)
            df.replace('-', -1, inplace=True)
            df.replace('½', .5, inplace=True, regex=True)
            df['Close'] = df['Close'].astype(float)
            df['ML'] = df['ML'].astype(float)

            while x < len(df):
                try:
                    if df['VH'].iloc[x] == 'V' and df['Close'].iloc[x] != -1:
                        if df['VH'].iloc[x + 1] == 'H' and df['Close'].iloc[x + 1] != -1:
                            away_team.append(df['Team'].iloc[x])
                            away_ml.append(df['ML'].iloc[x])
                            final_away.append(df['Final'].iloc[x])

                            home_team.append(df['Team'].iloc[x + 1])
                            home_ml.append(df['ML'].iloc[x + 1])
                            final_home.append(df['Final'].iloc[x + 1])

                            if df['Close'].iloc[x] > df['Close'].iloc[x + 1]:
                                spread.append(-df['Close'].iloc[x + 1])
                                ou.append(df['Close'].iloc[x])
                            else:
                                spread.append(df['Close'].iloc[x])
                                ou.append(df['Close'].iloc[x + 1])
                except Exception as e:
                    print(e)
                x += 1

        elif sport == 'MLB':
            while x < len(df):
                try:
                    if df['VH'].iloc[x] == 'V':
                        if df['VH'].iloc[x + 1] == 'H':
                            away_team.append(df['Team'].iloc[x])
                            home_team.append(df['Team'].iloc[x + 1])

                            away_ml.append(df['Close'].iloc[x])
                            home_ml.append(df['Close'].iloc[x + 1])

                            spread.append(df['RunLine'].iloc[x + 1])
                            ou.append(df['CloseOU'].iloc[x])

                            final_away.append(df['Final'].iloc[x])
                            final_home.append(df['Final'].iloc[x + 1])

                            away_pitcher.append(df['Pitcher'].iloc[x])
                            home_pitcher.append(df['Pitcher'].iloc[x + 1])

                except Exception as e:
                    print(e)
                x += 1
        elif sport == 'NHL':
            df[df['Final'] == 'F'] = -1

            while x < len(df):
                try:
                    if df['VH'].iloc[x] == 'V':
                        if df['VH'].iloc[x + 1] == 'H':
                            away_team.append(df['Team'].iloc[x])
                            home_team.append(df['Team'].iloc[x + 1])

                            away_ml.append(df['Close'].iloc[x])
                            home_ml.append(df['Close'].iloc[x + 1])

                            spread.append(df['PuckLine'].iloc[x + 1])
                            ou.append(df['CloseOU'].iloc[x])

                            final_away.append(df['Final'].iloc[x])
                            final_home.append(df['Final'].iloc[x + 1])

                except Exception as e:
                    print(e)
                x += 1
        elif sport == 'NBA':
            df = df.loc[:, :'2H']
            # df = df[:5000]
            df.replace('pk', 0, inplace=True)
            df.replace('PK', 0, inplace=True)
            df['Close'] = df['Close'].astype(float)

            for index, row in df.iterrows():
                if row['VH'] == 'V':
                    away_team.append(row['Team'])
                    away_ml.append(row['ML'])
                    final_away.append(row['Final'])
                    if row['Close'] < 50:
                        spread.append(row['Close'])
                    else:
                        ou.append(row['Close'])
                elif row['VH'] == 'H':
                    home_team.append(row['Team'])
                    home_ml.append(row['ML'])
                    final_home.append(row['Final'])
                    if row['Close'] < 50:
                        spread.append(-row['Close'])
                    else:
                        ou.append(row['Close'])
        elif sport in international_basketball_league:
            months = ['January', 'Feb']
            df = df[(df['Team'] != 'January') & (df['Team'] != 'February') & (df['Team'] != 'March') & (df['Team'] != 'April') & (df['Team'] != 'May') & (df['Team'] != 'June') & (df['Team'] != 'July') & (df['Team'] != 'August') & (df['Team'] != 'September') & (df['Team'] != 'October') & (df['Team'] != 'November') & (df['Team'] != 'December') & (df['PTS'] != 'PTS')]
            # df.drop(columns=['Unnamed: 3', 'Unnamed: 6', 'Unnamed: 8', 'OT'], inplace=True)
            df.rename(columns={'Team': 'Away_Team', 'PTS': 'Away_Final', 'Opp': 'Home_Team', 'PTS.1': 'Home_Final'}, inplace=True)

            df['Away_Team'] = df['Away_Team'].str.replace(r' ', '', regex=True)
            df['Home_Team'] = df['Home_Team'].str.replace(r' ', '', regex=True)

            while x < len(df):
                try:
                    away_team.append(df['Away_Team'].iloc[x])
                    home_team.append(df['Home_Team'].iloc[x])

                    away_ml.append(100)
                    home_ml.append(-110)

                    spread.append(5)
                    ou.append(150)

                    final_away.append(df['Away_Final'].iloc[x])
                    final_home.append(df['Home_Final'].iloc[x])

                except Exception as e:
                    print(e)
                x += 1

        elif sport == 'ATPTennis':
            df = df[['winner_name', 'loser_name', 'score', 'best_of']]
            df['score'] = df['score'].str.replace(r'RET', '0-0', regex=True).str.replace(r'DEF', '0-0', regex=True).str.replace(r'[', '', regex=True).str.replace(r']', '', regex=True).str.replace(r'Def.', '0-0', regex=True)
            df['score'] = df['score'].str.strip()
            df['1st_Set'] = df['score'].str.rsplit(' ').str[0].str.replace(r"\(.*\)", "", regex=True)
            df['winner_1st_points'] = df['1st_Set'].str.rsplit('-').str[0]
            df['loser_1st_points'] = df['1st_Set'].str.rsplit('-').str[1]
            df['2nd_Set'] = df['score'].str.rsplit(' ').str[1].str.replace(r"\(.*\)", "", regex=True)
            df['winner_2nd_points'] = df['2nd_Set'].str.rsplit('-').str[0]
            df['loser_2nd_points'] = df['2nd_Set'].str.rsplit('-').str[1]
            df['3rd_Set'] = df['score'].str.rsplit(' ').str[2].str.replace(r"\(.*\)", "", regex=True)
            df['winner_3rd_points'] = df['3rd_Set'].str.rsplit('-').str[0]
            df['loser_3rd_points'] = df['3rd_Set'].str.rsplit('-').str[1]
            df['4th_Set'] = df['score'].str.rsplit(' ').str[3].str.replace(r"\(.*\)", "", regex=True)
            df['winner_4th_points'] = df['4th_Set'].str.rsplit('-').str[0]
            df['loser_4th_points'] = df['4th_Set'].str.rsplit('-').str[1]
            df['5th_Set'] = df['score'].str.rsplit(' ').str[4].str.replace(r"\(.*\)", "", regex=True)
            df['winner_5th_points'] = df['5th_Set'].str.rsplit('-').str[0]
            df['loser_5th_points'] = df['5th_Set'].str.rsplit('-').str[1]

            winner_total = ['winner_1st_points', 'winner_2nd_points', 'winner_3rd_points', 'winner_4th_points',
                            'winner_5th_points']
            loser_total = ['loser_1st_points', 'loser_2nd_points', 'loser_3rd_points', 'loser_4th_points',
                           'loser_5th_points']
            df = df[(df['score'] != 'W/O') & (df['score'] != 'In Progress') & (df['score'] != 'Walkover')]
            df[winner_total] = df[winner_total].astype(float)
            df[loser_total] = df[loser_total].astype(float)
            df['Winner Total Points'] = df[winner_total].sum(axis=1)
            df['Loser Total Points'] = df[loser_total].sum(axis=1)
            df['Total'] = df['Winner Total Points'] + df['Loser Total Points']
            df['Ending_Spread'] = df['Winner Total Points'] - df['Loser Total Points']

            while x < len(df):
                try:
                    away_team.append(df['winner_name'].iloc[x])
                    home_team.append(df['loser_name'].iloc[x])

                    away_ml.append(100)
                    home_ml.append(-110)

                    spread.append(5)
                    ou.append(22)

                    final_away.append(df['Winner Total Points'].iloc[x])
                    final_home.append(df['Loser Total Points'].iloc[x])

                except Exception as e:
                    print(e)
                x += 1
        elif sport == 'WNBA':
            df = df.drop(columns=['2', '6'])
            df['Away_Team'] = df['1'].str.rsplit(' - ', 1).str[0]
            df['Away_Team'] = df['Away_Team'].str.replace(' W', '')
            df['Home_Team'] = df['1'].str.rsplit(' - ', 1).str[1]
            df['Home_Team'] = df['Home_Team'].str.replace(' W', '')
            df['3'] = df['3'].str.replace('OT', '')
            df = df[(df['3'] != 'award.') & (df['3'] != 'canc.') & (df['3'] != 'w.o.') & (df['3'] != 'ret.')]
            df['3'] = df['3'].str.strip()
            df['Away_Final'] = df['3'].str.rsplit(':', 1).str[0]
            df['Home_Final'] = df['3'].str.rsplit(':', 1).str[1]
            df['4'] = df['4'].replace('-', '100')
            df['5'] = df['5'].replace('-', '100')

            while x < len(df):
                try:
                    away_team.append(df['Away_Team'].iloc[x])
                    home_team.append(df['Home_Team'].iloc[x])

                    away_ml.append(df['4'].iloc[x])
                    home_ml.append(df['5'].iloc[x])

                    spread.append(5)
                    ou.append(155)

                    final_away.append(df['Away_Final'].iloc[x])
                    final_home.append(df['Home_Final'].iloc[x])

                except Exception as e:
                    print(e)
                x += 1

        elif sport in oddsportal_tennis:
            df = df[(df['3'] != 'award.') & (df['3'] != 'canc.') & (df['3'] != 'w.o.') & (df['3'] != 'ret.') & (df['3'] != 'abn.')]
            df = df[(df['1'] != df['3'])]
            df = df.drop(columns=['2', '6'])
            df['Away_Team'] = df['1'].str.rsplit(' - ', 1).str[0]
            df['Away_Team'] = df['Away_Team'].str.rsplit(' ', 1).str[0]
            df['Home_Team'] = df['1'].str.rsplit(' - ', 1).str[1]
            df['Home_Team'] = df['Home_Team'].str.rsplit(' ', 1).str[0]
            df['Away_Final'] = df['3'].str.rsplit(':', 1).str[0]
            df['Home_Final'] = df['3'].str.rsplit(':', 1).str[1]
            df['4'] = df['4'].replace('-', '100')
            df['5'] = df['5'].replace('-', '100')

            while x < len(df):
                try:
                    away_team.append(df['Away_Team'].iloc[x])
                    home_team.append(df['Home_Team'].iloc[x])

                    away_ml.append(df['4'].iloc[x])
                    home_ml.append(df['5'].iloc[x])

                    spread.append(-1)
                    ou.append(2.5)

                    final_away.append(df['Away_Final'].iloc[x])
                    final_home.append(df['Home_Final'].iloc[x])

                except Exception as e:
                    print(e)
                x += 1

        elif sport in ['CPBL', 'NPB', 'MexicanLMB', 'KBO']:
            df = df[(df['3'] != 'postp.') & (df['4'] != '-') & (df['3'] != 'canc.') & (df['3'] != df['2'])]

            df['Away_Team'] = df['1'].str.rsplit(' - ', 1).str[0]
            df['Away_Team'] = df['Away_Team'].str.rsplit(' ', 1).str[0]
            df['Home_Team'] = df['1'].str.rsplit(' - ', 1).str[1]
            df['Home_Team'] = df['Home_Team'].str.rsplit(' ', 1).str[0]
            df['Away_Final'] = df['3'].str.rsplit(':', 1).str[0]
            df['Home_Final'] = df['3'].str.rsplit(':', 1).str[1]
            df['4'] = df['4'].replace('-', '100')
            df['5'] = df['5'].replace('-', '100')

            print(df)

            while x < len(df):
                try:
                    away_team.append(df['Away_Team'].iloc[x])
                    home_team.append(df['Home_Team'].iloc[x])

                    away_ml.append(df['4'].iloc[x])
                    home_ml.append(df['5'].iloc[x])

                    spread.append(-1)
                    ou.append(2.5)

                    final_away.append(df['Away_Final'].iloc[x])
                    final_home.append(df['Home_Final'].iloc[x])

                except Exception as e:
                    print(e)
                x += 1

        elif sport in soccer:
            df = df.drop(columns=['6'])
            df = df[(df['1'] != df['2']) & (df['3'] != '-') & (df['2'] != 'award.') & (df['2'] != 'canc.') & (df['2'] != 'postp.')]
            print(df)
            df['Away_Team'] = df['1'].str.rsplit(' - ', 1).str[0]
            df['Home_Team'] = df['1'].str.rsplit(' - ', 1).str[1]

            df['2'] = df['2'].str.replace('pen.', '').str.strip().str.replace('ET', '').str.strip()
            df['Away_Final'] = df['2'].str.rsplit(':', 1).str[0]
            df['Home_Final'] = df['2'].str.rsplit(':', 1).str[1]

            while x < len(df):
                try:
                    away_team.append(df['Away_Team'].iloc[x])
                    home_team.append(df['Home_Team'].iloc[x])

                    away_ml.append(df['3'].iloc[x])
                    home_ml.append(df['5'].iloc[x])
                    draw_ml.append(df['4'].iloc[x])

                    spread.append(-1)
                    ou.append(2.5)

                    final_away.append(df['Away_Final'].iloc[x])
                    final_home.append(df['Home_Final'].iloc[x])

                except Exception as e:
                    print(e)
                x += 1

        data = {'Home_Team': home_team,
                'Away_Team': away_team,
                'Home_ML': home_ml,
                'Away_ML': away_ml,
                'Spread': spread,
                'Over/Under': ou,
                'Home_Final': final_home,
                'Away_Final': final_away}

        if sport in soccer:
            data['Draw_ML'] = draw_ml

        final = pd.DataFrame(data)
        final = final[final['Home_Final'] != 'NL']
        final['Home_Final'] = final['Home_Final'].astype(float)
        final['Away_Final'] = final['Away_Final'].astype(float)
        final['Home_ML'] = final['Home_ML'].astype(float)
        final['Away_ML'] = final['Away_ML'].astype(float)
        if sport in soccer:
            final['Draw_ML'] = final['Draw_ML'].astype(float)
        final['Total'] = final['Home_Final'] + final['Away_Final']
        final['Over/Under'] = final['Over/Under'].replace('½', .5, regex=True).astype(float)
        final['Ending_Spread'] = final['Away_Final'] - final['Home_Final']
        final = final[final['Home_ML'] != 0]
        final = final[final['Away_ML'] != 0]
        final = final[final['Spread'] != 0]

        for index, row in final.iterrows():
            if row['Home_ML'] > 0:
                final.loc[index, 'Home_ML'] = ((row['Home_ML'] / 100) + 1)
            else:
                final.loc[index, 'Home_ML'] = (1 - (100 / row['Home_ML']))

            if row['Away_ML'] > 0:
                final.loc[index, 'Away_ML'] = ((row['Away_ML'] / 100) + 1)
            else:
                final.loc[index, 'Away_ML'] = (1 - (100 / row['Away_ML']))

            if sport in soccer:
                if row['Draw_ML'] > 0:
                    final.loc[index, 'Draw_ML'] = ((row['Draw_ML'] / 100) + 1)
                else:
                    final.loc[index, 'Draw_ML'] = (1 - (100 / row['Draw_ML']))

        final = final[final['Home_ML'] != 101]

        final.to_sql(f'Cleaned {season} {sport} Odds', self.engine, index=False, if_exists='replace')

        print(final)
        return final

    def combine_odds_stats(self, season, sport):
        odds = pd.read_sql_table(f'Cleaned {season} {sport} Odds', con=self.engine)
        stats = pd.read_sql_table(f'Cleaned {season} {sport} Stats', con=self.engine)
        print(odds)
        print(stats)
        on_home = odds.merge(stats, left_on='Home_Team', right_on='Team')
        combined_df = on_home.merge(stats, left_on='Away_Team', right_on='Team')
        combined_df['Year'] = season

        print(combined_df)

        combined_df.to_sql(f'{season} {sport} Matched', self.engine, index=False, if_exists='replace')

        return combined_df

    def combine_leagues(self, sport, season, **kwargs):
        stats = kwargs.get('stats', False)
        df = pd.DataFrame()
        atp_clay = ['ATPRome', 'ATPGeneva', 'ATPLyon', 'ATPFrenchOpen']
        wta_clay = ['WTARome', 'WTAStrasbourg', 'WTARabat']
        if stats is False:
            if sport == 'ATPClay':
                for city in atp_clay:
                    final = pd.read_sql_table(f'{season} {city} Matched', con=self.engine)
                    final['Year'] = season
                    df = pd.concat([df, final])
            df = df.groupby(['Team'], as_index=False).sum()
            df.to_sql(f'Combined {season} {sport} Data', self.engine, index=False, if_exists='replace')
        else:
            if sport == 'ATPClay':
                for city in atp_clay:
                    final = pd.read_sql_table(f'Cleaned {season} {city} Stats', con=self.engine)
                    final['Year'] = season
                    df = pd.concat([df, final])
            print(df)
            df = df.groupby(['Tm'], as_index=False).sum()
            df.to_sql(f'Combined {season} {sport} Stats', self.engine, index=False, if_exists='replace')

    def combine_seasons(self, sport):
        seasons = ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022']
        df = pd.DataFrame()
        for season in seasons:
            try:
                if sport == 'ATPClay':
                    try:
                        final = pd.read_sql_table(f'Combined {season} {sport} Data', con=self.engine)
                    except Exception as e:
                        for season in seasons:
                            self.combine_leagues(sport, season)
                        final = pd.read_sql_table(f'Combined {season} {sport} Data', con=self.engine)
                    final['Year'] = season
                    df = pd.concat([df, final])
                else:
                    try:
                        final = pd.read_sql_table(f'{season} {sport} Matched', con=self.engine)
                        # print(final)
                        final['Year'] = season
                        df = pd.concat([df, final])
                    except:
                        pass
            except Exception as e:
                print(e)

        print(df)

        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        print(df)
        df.to_sql(f'Consolidated {sport} Data', self.engine, index=False, if_exists='replace')

        return df

    def live_data(self, season, sport, sportsbook):
        international_basketball_league = ['EuroLeague', 'EuroCup', 'NBLAustralia', 'FranceLNB', 'VTBUnited',
                                           'SpainACB', 'GreekBasketball', 'ItalyLega']
        clay_tennis = ['ATPRome', 'ATPGeneva', 'ATPLyon', 'ATPFrenchOpen']

        if sportsbook == 'Betonline':
            live_spread = self.scraper.get_betonline_lines(sport)
        elif sportsbook == 'Bovada':
            live_spread = self.scraper.get_bovada_spread(sport)
        live_spread['Home_Team'] = live_spread['Home_Team'].str.replace(' ', '').str.replace('-Game#1', '').str.replace('-Game#2', '')
        live_spread['Away_Team'] = live_spread['Away_Team'].str.replace(' ', '').str.replace('-Game#1', '').str.replace('-Game#2', '')

        if sport in clay_tennis:
            live_spread['Home_Team'] = live_spread['Home_Team'].str.rsplit(',', 1).str[0]
            live_spread['Away_Team'] = live_spread['Away_Team'].str.rsplit(',', 1).str[0]
            self.combine_leagues('ATPClay', season, stats=True)
            stats = pd.read_sql_table(f'Combined {season} ATPClay Stats', con=self.engine)
        else:
            self.clean_sr_stats(sport, season, rerun=True)
            stats = pd.read_sql_table(f'Cleaned {season} {sport} Stats', con=self.engine)

        print(stats)

        stats['Tm'] = stats['Tm'].str.replace(' ', '')
        print(live_spread)
        print(stats)
        on_home = live_spread.merge(stats, left_on='Home_Team', right_on='Tm')
        combined_df = on_home.merge(stats, left_on='Away_Team', right_on='Tm')
        combined_df['Year'] = season
        combined_df.drop(columns=['Tm_x', 'Tm_y'], axis=1)
        # if sport in clay_tennis:
            # combined_df = combined_df.drop(columns=['Year_x', 'Year_y'])

        print(combined_df)

        combined_df.to_sql(f'Live {sport} Data', self.engine, index=False, if_exists='replace')
        return combined_df

    def manual_data(self, season, sport, home_team, away_team):
        stats = pd.read_sql_table(f'Cleaned {season} {sport} Stats', con=self.engine)
        home_stats = stats[stats['Tm'] == home_team]
        away_stats = stats[stats['Tm'] == away_team]
        combined_df = home_stats.merge(away_stats, how='cross')
        reversed_combined_df = away_stats.merge(home_stats, how='cross')
        combined_df = pd.concat([combined_df, reversed_combined_df])
        combined_df.rename(columns={'Tm_x': 'Away_Team', 'Tm_y': 'Home_Team'}, inplace=True)
        print(combined_df)

        combined_df.to_sql(f'Manual {sport} Data', self.engine, index=False, if_exists='replace')
        return combined_df

    def refresh(self, sport):
        seasons = ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022']
        # self.scraper.get_odds_table(sport)
        for season in seasons:
            try:
                self.clean_sr_stats(sport, season, rerun=True, multipage=True)
                self.clean_odds(sport, season, rerun=True)
                self.combine_odds_stats(season, sport)
            except Exception as e:
                print(e)
        self.combine_seasons(sport)
