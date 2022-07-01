import pandas as pd
import sqlalchemy as sql
from pathlib import Path
import os

from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import model_from_json

import seaborn as sns
import matplotlib.pyplot as plt

from Scraping.scraping import Scraper
from Scraping.index import *

pd.set_option('display.max_columns', 2000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 10000)


class Model:
    def __init__(self):
        self.scraper = Scraper()
        self.engine = self.scraper.engine
        self.columns_to_drop = ['Home_Team', 'Away_Team', 'Spread', 'Over/Under', 'Home_ML', 'Away_ML', 'Year', 'Team_y', 'Team_x', 'Tm_x', 'Tm_y', 'Ending_Spread', 'Home_Final', 'Away_Final', 'Total']

    def build_model(self, sport, **kwargs):
        other_drop_columns = kwargs.get('drop_columns', [])
        season = kwargs.get('season', '')

        international_basketball_league = ['EuroLeague', 'EuroCup', 'NBLAustralia', 'FranceLNB', 'VTBUnited', 'SpainACB', 'GreekBasketball', 'ItalyLega']
        soccer = leagues['Soccer']

        oddsportal_sports = ['ATPRome', 'WNBA', 'WTARome', 'ATPLyon', 'ATPFrenchOpen']

        if sport == 'NBA':
            self.columns_to_drop = self.columns_to_drop + ['City_x', 'City_y', 'Mascot_x', 'Mascot_y', 'Rk_x', 'Rk_y', 'G_y', 'G_x']
        elif sport == 'NHL':
            self.columns_to_drop = self.columns_to_drop + ['Mascot_x', 'Mascot_y']
        elif sport in international_basketball_league:
            self.columns_to_drop = self.columns_to_drop + ['Rk_x', 'Rk_y', 'Club_x', 'Club_y']
        elif sport in soccer:
            self.columns_to_drop = self.columns_to_drop + ['Draw_ML']
        # elif sport == 'ATPRome' or 'WNBA':
            # self.columns_to_drop.remove('Tm_x')
            # self.columns_to_drop.remove('Tm_y')

        if season != '':
            a = pd.read_sql_table(f'{season} {sport} Matched', con=self.engine)
        else:
            a = pd.read_sql_table(f'Consolidated {sport} Data', con=self.engine)

        df = a[a['Spread'] != 0]
        df = df[df['Year'] != '2022']

        home_y = df['Home_Final'].values
        away_y = df['Away_Final'].values

        self.columns_to_drop = self.columns_to_drop + other_drop_columns
        print(df)
        print(self.columns_to_drop)
        X = df.drop(columns=self.columns_to_drop)
        print(X.head())

        scalar = StandardScaler()
        X = scalar.fit_transform(X)
        x_train, x_valid, y_train, y_valid = train_test_split(X, home_y, test_size=0.3, random_state=42)

        number_input_features = X.shape[1]
        nn = Sequential()

        nn.add(Dense(units=number_input_features, input_dim=number_input_features, activation="relu"))
        nn.add(Dense(units=number_input_features * 2, activation="relu"))
        nn.add(Dense(units=number_input_features, activation="relu"))
        nn.add(Dense(units=1, activation="linear"))
        nn.compile(loss="mean_absolute_error", optimizer="adam", metrics=["mae"])
        nn.fit(x_train, y_train, validation_data=[x_valid, y_valid], epochs=25, verbose=1)

        nn_json = nn.to_json()
        Path(f"Scraping/Model/{sport}").mkdir(parents=True, exist_ok=True)
        model_file_path = Path(f"Scraping/Model/{sport}/{sport}_Home_model.json")
        with open(model_file_path, "w") as json_file:
            json_file.write(nn_json)
        weights_file_path = Path(f"Scraping/Model/{sport}/{sport}_Home_weights.h5")
        nn.save_weights(weights_file_path)

        nn = Sequential()
        x_train, x_valid, y_train, y_valid = train_test_split(X, away_y, test_size=0.3, random_state=42)

        nn.add(Dense(units=number_input_features, input_dim=number_input_features, activation="relu"))
        nn.add(Dense(units=number_input_features * 2, activation="relu"))
        nn.add(Dense(units=number_input_features, activation="relu"))
        nn.add(Dense(units=1, activation="linear"))
        nn.compile(loss="mean_absolute_error", optimizer="adam", metrics=["mae"])
        nn.fit(x_train, y_train, validation_data=[x_valid, y_valid], epochs=25, verbose=1)

        nn_json = nn.to_json()
        model_file_path = Path(f"Scraping/Model/{sport}/{sport}_Away_model.json")
        with open(model_file_path, "w") as json_file:
            json_file.write(nn_json)
        weights_file_path = Path(f"Scraping/Model/{sport}/{sport}_Away_weights.h5")
        nn.save_weights(weights_file_path)

    def load_backtest_model(self, sport, **kwargs):
        other_drop_columns = kwargs.get('drop_columns', [])
        season = kwargs.get('season', '')
        live = kwargs.get('live', False)
        manual = kwargs.get('manual', False)

        international_basketball_league = ['EuroLeague', 'EuroCup', 'NBLAustralia', 'FranceLNB', 'VTBUnited', 'SpainACB', 'GreekBasketball', 'ItalyLega']
        clay_tennis = ['ATPRome', 'ATPGeneva', 'ATPLyon', 'ATPFrenchOpen']
        soccer = leagues['Soccer']

        if sport == 'NBA':
            self.columns_to_drop = self.columns_to_drop + ['City_x', 'City_y', 'Mascot_x', 'Mascot_y', 'Rk_x', 'Rk_y', 'G_y', 'G_x']
        elif sport == 'NHL':
            self.columns_to_drop = self.columns_to_drop + ['Mascot_x', 'Mascot_y']
        elif sport in international_basketball_league:
            self.columns_to_drop = self.columns_to_drop + ['Rk_x', 'Rk_y', 'Club_x', 'Club_y']
        elif sport in soccer:
            self.columns_to_drop = self.columns_to_drop + ['Draw_ML']
        # elif sport in clay_tennis:
            # self.columns_to_drop.remove('Team_x')
            # self.columns_to_drop.remove('Team_y')

        if live is True:
            df = pd.read_sql_table(f'Live {sport} Data', con=self.engine)
            self.columns_to_drop = [e for e in self.columns_to_drop if e not in ('Ending_Spread', 'Home_Final', 'Away_Final', 'Total')]
            self.columns_to_drop = self.columns_to_drop + ['Date']
            if sport in clay_tennis:
                self.columns_to_drop.remove('Team_x')
                self.columns_to_drop.remove('Team_y')
        else:
            if manual is True:
                df = pd.read_sql_table(f'Manual {sport} Data', con=self.engine)
                self.columns_to_drop = ['Team_x', 'Team_y', 'Home_Team', 'Away_Team']

            elif season != '':
                df = pd.read_sql_table(f'{season} {sport} Matched', con=self.engine)
            else:
                df = pd.read_sql_table(f'Consolidated {sport} Data', con=self.engine)

            df = df[df['Spread'] != 0]

        model_file_path = Path(f"Scraping/Model/{sport}/{sport}_Away_model.json")
        with open(model_file_path, "r") as json_file:
            model_json = json_file.read()
        loaded_model = model_from_json(model_json)
        weights_file_path = Path(f"Scraping/Model/{sport}/{sport}_Away_weights.h5")
        loaded_model.load_weights(weights_file_path)

        self.columns_to_drop = self.columns_to_drop + other_drop_columns
        df.fillna(0, inplace=True)
        test_X = df.drop(columns=self.columns_to_drop)
        scaler = StandardScaler().fit(test_X)
        test_X = scaler.transform(test_X)

        final = pd.DataFrame()
        final['Away_Team'] = df['Away_Team']
        final['Home_Team'] = df['Home_Team']

        if manual is False:
            final['Away_ML'] = df['Away_ML']
            final['Home_ML'] = df['Home_ML']
            if sport in soccer:
                final['Draw_ML'] = df['Draw_ML']
            final['Spread'] = df['Spread']
            if live is False:
                final['Home_Final'] = df['Home_Final']
                final['Away_Final'] = df['Away_Final']
                final['Actual_Spread'] = df['Ending_Spread']
                final['Total'] = df['Total']
                final['Year'] = df['Year']
            else:
                final['Date'] = df['Date']
            final['Over/Under'] = df['Over/Under']
        final['Predicted_Away'] = loaded_model.predict(test_X)

        model_file_path = Path(f"Scraping/Model/{sport}/{sport}_Home_model.json")
        with open(model_file_path, "r") as json_file:
            model_json = json_file.read()
        loaded_model = model_from_json(model_json)
        weights_file_path = Path(f"Scraping/Model/{sport}/{sport}_Home_weights.h5")
        loaded_model.load_weights(weights_file_path)

        final['Predicted_Home'] = loaded_model.predict(test_X)
        final['Predicted_Spread'] = final['Predicted_Away'] - final['Predicted_Home']
        final['Predicted_Total'] = final['Predicted_Away'] + final['Predicted_Home']

        print(final.tail(200))

        final = final[(final['Predicted_Home'] > 0) & (final['Predicted_Away'] > 0)]

        if live is True:
            final.to_sql(f'Live {sport}', self.engine, index=False, if_exists='replace')
        else:
            if season != '':
                final.to_sql(f'{season} {sport} Backtest', self.engine, index=False, if_exists='replace')
            else:
                final.to_sql(f'Consolidated {sport} Backtest', self.engine, index=False, if_exists='replace')

        return final

    def backtest_stats(self, sport, **kwargs):
        season = kwargs.get('season', '')
        soccer = leagues['Soccer']
        if season == '':
            final = pd.read_sql_table(f'Consolidated {sport} Backtest', con=self.engine)
        else:
            final = pd.read_sql_table(f'{season} {sport} Backtest', con=self.engine)

        for index, row in final.iterrows():
            if min(abs(round(row['Predicted_Spread'] / row['Spread'])), 5) > 10:
                bet_amount = 1
            elif min(abs(round(row['Predicted_Spread'] / row['Spread'])), 5) > 5:
                bet_amount = 1
            elif min(abs(round(row['Predicted_Spread'] / row['Spread'])), 5) > 3:
                bet_amount = 1
            elif min(abs(round(row['Predicted_Spread'] / row['Spread'])), 5) > 2:
                bet_amount = 1
            else:
                bet_amount = 1
            if row['Predicted_Spread'] < 0 > row['Spread']:
                if abs(row['Predicted_Spread']) > abs(row['Spread']):
                    if row['Actual_Spread'] < row['Spread']:
                        final.loc[index, 'Win/Loss'] = 'Win'
                        final.loc[index, 'Spread_Profit/Loss'] = 0.9 * bet_amount
                        final.loc[index, 'Spread_Volume'] = bet_amount
                    elif row['Actual_Spread'] > row['Spread']:
                        final.loc[index, 'Win/Loss'] = 'Loss'
                        final.loc[index, 'Spread_Profit/Loss'] = -bet_amount
                        final.loc[index, 'Spread_Volume'] = bet_amount
                    else:
                        final.loc[index, 'Win/Loss'] = 'Push'

                elif abs(row['Predicted_Spread']) < abs(row['Spread']):
                    if row['Actual_Spread'] > row['Spread']:
                        final.loc[index, 'Win/Loss'] = 'Win'
                        final.loc[index, 'Spread_Profit/Loss'] = 0.9 * bet_amount
                        final.loc[index, 'Spread_Volume'] = bet_amount
                    elif row['Actual_Spread'] < row['Spread']:
                        final.loc[index, 'Win/Loss'] = 'Loss'
                        final.loc[index, 'Spread_Profit/Loss'] = -bet_amount
                        final.loc[index, 'Spread_Volume'] = bet_amount
                    else:
                        final.loc[index, 'Win/Loss'] = 'Push'

            elif row['Predicted_Spread'] > 0 < row['Spread']:
                if abs(row['Predicted_Spread']) > abs(row['Spread']):
                    if row['Actual_Spread'] > row['Spread']:
                        final.loc[index, 'Win/Loss'] = 'Win'
                        final.loc[index, 'Spread_Profit/Loss'] = 0.9 * bet_amount
                        final.loc[index, 'Spread_Volume'] = bet_amount
                    elif row['Actual_Spread'] < row['Spread']:
                        final.loc[index, 'Win/Loss'] = 'Loss'
                        final.loc[index, 'Spread_Profit/Loss'] = -bet_amount
                        final.loc[index, 'Spread_Volume'] = bet_amount
                    else:
                        final.loc[index, 'Win/Loss'] = 'Push'

                elif abs(row['Predicted_Spread']) < abs(row['Spread']):
                    if row['Actual_Spread'] < row['Spread']:
                        final.loc[index, 'Win/Loss'] = 'Win'
                        final.loc[index, 'Spread_Profit/Loss'] = 0.9 * bet_amount
                        final.loc[index, 'Spread_Volume'] = bet_amount
                    elif row['Actual_Spread'] > row['Spread']:
                        final.loc[index, 'Win/Loss'] = 'Loss'
                        final.loc[index, 'Spread_Profit/Loss'] = -bet_amount
                        final.loc[index, 'Spread_Volume'] = bet_amount
                    else:
                        final.loc[index, 'Win/Loss'] = 'Push'

            elif row['Predicted_Spread'] > 0 > row['Spread']:
                if row['Actual_Spread'] > row['Spread']:
                    final.loc[index, 'Win/Loss'] = 'Win'
                    final.loc[index, 'Spread_Profit/Loss'] = 0.9 * bet_amount
                    final.loc[index, 'Spread_Volume'] = bet_amount
                elif row['Actual_Spread'] < row['Spread']:
                    final.loc[index, 'Win/Loss'] = 'Loss'
                    final.loc[index, 'Spread_Profit/Loss'] = -bet_amount
                    final.loc[index, 'Spread_Volume'] = bet_amount
                else:
                    final.loc[index, 'Win/Loss'] = 'Push'
            elif row['Predicted_Spread'] < 0 < row['Spread']:
                if row['Actual_Spread'] < row['Spread']:
                    final.loc[index, 'Win/Loss'] = 'Win'
                    final.loc[index, 'Spread_Profit/Loss'] = 0.9 * bet_amount
                    final.loc[index, 'Spread_Volume'] = bet_amount
                elif row['Actual_Spread'] > row['Spread']:
                    final.loc[index, 'Win/Loss'] = 'Loss'
                    final.loc[index, 'Spread_Profit/Loss'] = -bet_amount
                    final.loc[index, 'Spread_Volume'] = bet_amount
                else:
                    final.loc[index, 'Win/Loss'] = 'Push'
            if sport in soccer and (-0.25 < row['Predicted_Spread'] < 0.25):
                if row['Actual_Spread'] == 0:
                    final.loc[index, 'ML_Win/Loss'] = 'Win'
                    final.loc[index, 'ML_Profit/Loss'] = (row['Draw_ML'] - 1)
                else:
                    final.loc[index, 'ML_Win/Loss'] = 'Loss'
                    final.loc[index, 'ML_Profit/Loss'] = -1
                final.loc[index, 'ML_Volume'] = 1
            elif row['Predicted_Spread'] > 0:
                if row['Actual_Spread'] > 0:
                    final.loc[index, 'ML_Win/Loss'] = 'Win'
                    final.loc[index, 'ML_Profit/Loss'] = (row['Away_ML'] - 1)
                else:
                    final.loc[index, 'ML_Win/Loss'] = 'Loss'
                    final.loc[index, 'ML_Profit/Loss'] = -1
                final.loc[index, 'ML_Volume'] = 1
            elif row['Predicted_Spread'] < 0:
                if row['Actual_Spread'] < 0:
                    final.loc[index, 'ML_Win/Loss'] = 'Win'
                    final.loc[index, 'ML_Profit/Loss'] = (row['Home_ML'] - 1)
                else:
                    final.loc[index, 'ML_Win/Loss'] = 'Loss'
                    final.loc[index, 'ML_Profit/Loss'] = -1
                final.loc[index, 'ML_Volume'] = 1

            if row['Predicted_Total'] > row['Over/Under']:
                if row['Total'] > row['Over/Under']:
                    final.loc[index, 'Total_Win/Loss'] = 'Win'
                    final.loc[index, 'Total_Profit/Loss'] = 0.9
                elif row['Total'] < row['Over/Under']:
                    final.loc[index, 'Total_Win/Loss'] = 'Loss'
                    final.loc[index, 'Total_Profit/Loss'] = -1
                final.loc[index, 'Total_Volume'] = 1
            elif row['Predicted_Total'] < row['Over/Under']:
                if row['Total'] < row['Over/Under']:
                    final.loc[index, 'Total_Win/Loss'] = 'Win'
                    final.loc[index, 'Total_Profit/Loss'] = 0.9
                elif row['Total'] > row['Over/Under']:
                    final.loc[index, 'Total_Win/Loss'] = 'Loss'
                    final.loc[index, 'Total_Profit/Loss'] = -1
                final.loc[index, 'Total_Volume'] = 1

        final.update(final[['Spread_Volume', 'Spread_Profit/Loss', 'ML_Volume', 'ML_Profit/Loss', 'Total_Volume', 'Total_Profit/Loss']].fillna(0))

        print(final)

        final.to_sql(f'{season} {sport} Backtest Stats', self.engine, index=False, if_exists='replace')

        return final

    def refresh_model(self, sport):
        columns_to_drop = []
        self.build_model(sport, drop_columns=columns_to_drop)
        self.load_backtest_model(sport, drop_columns=columns_to_drop)
        self.backtest_stats(sport)
