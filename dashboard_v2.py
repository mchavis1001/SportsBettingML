import pandas as pd
import streamlit as st
import plotly.express as px

from Scraping.cleaning_data import CleanData
from Scraping.scraping import Scraper
from Scraping.model import Model

from Scraping.index import leagues

data = CleanData()
scraper = Scraper()
model = Model()

st.set_page_config(layout='wide')

with st.sidebar:
    selected_sport = st.selectbox('Select Sport', ('Basketball', 'Baseball', 'Hockey', 'Football', 'Soccer', 'Tennis'))
    selected_league = st.selectbox('Select League', leagues[selected_sport])
    select_run_script = st.selectbox('Select Run', ('Backtest', 'Live', 'Manual', 'Data'))
    # sport_button = st.form_submit_button(f'Go')

# with st.sidebar.form('Sidebar'):
    # selected_league = st.selectbox('Select League', leagues[selected_sport])
    # select_run_script = st.selectbox('Select Run', ('Backtest', 'Live', 'Testing', 'Manual', 'Data'))
    # run_button = st.form_submit_button(f'Go')

st.title(f'{selected_league} {select_run_script}')


# @st.cache
def backtest(sport):
    with st.spinner('Wait for it...'):
        try:
            # final = model.backtest_stats(sport)
            final = pd.read_sql_table(f' {sport} Backtest Stats', con=model.engine)
        except Exception:
            data.refresh(sport)
            data.combine_seasons(sport)
            model.build_model(sport)
            model.load_backtest_model(sport)
            final = model.backtest_stats(sport)

        # with st.sidebar:
            # chart_dropdown = st.selectbox('Select Bet Type', ('Spread', 'MoneyLine', 'Total'))
            # chart_button = st.form_submit_button('Display')

        with st.sidebar.form('Backtest Filters'):
            chart_dropdown = st.selectbox('Select Bet Type', ('Spread', 'MoneyLine', 'Total'))
            year = st.select_slider('Select Year', final['Year'].sort_values(ascending=True), value=(final['Year'].min(), final['Year'].max()))
            home_odds = st.select_slider('Home Odds', round(final['Home_ML'].sort_values(ascending=True), 2), value=(round(final['Home_ML'].min(), 2), round(final['Home_ML'].max(), 2)))
            away_odds = st.select_slider('Away Odds', round(final['Away_ML'].sort_values(ascending=True), 2), value=(round(final['Away_ML'].min(), 2), round(final['Away_ML'].max(), 2)))
            book_spread = st.select_slider('Spread', round(final['Spread'].sort_values(ascending=True), 2), value=(round(final['Spread'].min(), 2), round(final['Spread'].max(), 2)))
            prediction = st.select_slider('Model Prediction', round(final['Predicted_Spread'].sort_values(ascending=True), 2), value=(round(final['Predicted_Spread'].min(), 2), round(final['Predicted_Spread'].max(), 2)))
            run_results = st.form_submit_button('Filter')

        with st.sidebar.form('Refresh'):
            refresh_model = st.form_submit_button('Refresh Model')
            refresh_data = st.form_submit_button('Refresh Data')

        if refresh_model:
            model.refresh_model(sport)

        elif refresh_data:
            data.refresh(sport)

        elif run_results:
            final = final[(final['Year'] >= year[0]) & (final['Year'] <= year[1])]
            final = final[(final['Spread'] >= book_spread[0]) & (final['Spread'] <= book_spread[1])]
            final = final[(final['Home_ML'] >= home_odds[0]) & (final['Home_ML'] <= home_odds[1])]
            final = final[(final['Away_ML'] >= away_odds[0]) & (final['Away_ML'] <= away_odds[1])]
            final = final[(final['Predicted_Spread'] >= prediction[0]) & (final['Predicted_Spread'] <= prediction[1])]

        final['Cumulative_Spread_Volume'] = final['Spread_Volume'].cumsum()
        final['Cumulative_Spread_Profit/Loss'] = final['Spread_Profit/Loss'].cumsum()
        final['Cumulative_Total_Volume'] = final['Total_Volume'].cumsum()
        final['Cumulative_Total_Profit/Loss'] = final['Total_Profit/Loss'].cumsum()
        final['Cumulative_ML_Volume'] = final['ML_Volume'].cumsum()
        final['Cumulative_ML_Profit/Loss'] = final['ML_Profit/Loss'].cumsum()
        final['Combine_Volume'] = final['Cumulative_Spread_Volume'] + final['Cumulative_ML_Volume'] + final['Cumulative_Total_Volume']
        final['Combine_Profit/Loss'] = final['Cumulative_Spread_Profit/Loss'] + final['Cumulative_ML_Profit/Loss'] + final['Cumulative_Total_Profit/Loss']
        final['Combine_HV'] = final['Combine_Profit/Loss'].cummax()
        final['Combine_Drawdown'] = final['Combine_Profit/Loss'] - final['Combine_HV']
        final['ML_HighValue'] = final['Cumulative_ML_Profit/Loss'].cummax()
        final['ML_Drawdown'] = final['Cumulative_ML_Profit/Loss'] - final['ML_HighValue']
        final['Spread_HighValue'] = final['Cumulative_Spread_Profit/Loss'].cummax()
        final['Spread_Drawdown'] = final['Cumulative_Spread_Profit/Loss'] - final['Spread_HighValue']
        final['Total_HighValue'] = final['Cumulative_Total_Profit/Loss'].cummax()
        final['Total_Drawdown'] = final['Cumulative_Total_Profit/Loss'] - final['Total_HighValue']
        final.reset_index(drop=True, inplace=True)

        spread_wins = final['Win/Loss'].value_counts()['Win']
        spread_losses = final['Win/Loss'].value_counts()['Loss']
        ml_wins = final['ML_Win/Loss'].value_counts()['Win']
        ml_losses = final['ML_Win/Loss'].value_counts()['Loss']
        total_wins = final['Total_Win/Loss'].value_counts()['Win']
        total_losses = final['Total_Win/Loss'].value_counts()['Loss']
        spread_win_percentage = round(spread_wins / (spread_losses + spread_wins) * 100, 2)
        ml_win_percentage = round(ml_wins / (ml_wins + ml_losses) * 100, 2)
        total_win_percentage = round(total_wins / (total_wins + total_losses) * 100, 2)
        spread_ev = final['Cumulative_Spread_Profit/Loss'].iloc[-1] / final['Cumulative_Spread_Volume'].iloc[-1] * 100
        ml_ev = round(final.iloc[-1]['Cumulative_ML_Profit/Loss'] / final.iloc[-1]['Cumulative_ML_Volume'] * 100, 2)
        total_ev = round(final.iloc[-1]['Cumulative_Total_Profit/Loss'] / final.iloc[-1]['Cumulative_Total_Volume'] * 100, 2)

        col1, col2 = st.columns([2.5, 1])
        col3, col4 = st.columns([2.5, 1])

        with col1:
            # if chart_button:
                st.header(f'Cumulative {chart_dropdown} Profit/Loss')
                if chart_dropdown == 'Spread':
                    fig = px.line(final, x=final.index, y="Cumulative_Spread_Profit/Loss")

                elif chart_dropdown == 'MoneyLine':
                    fig = px.line(final, x=final.index, y="Cumulative_ML_Profit/Loss")

                elif chart_dropdown == 'Total':
                    fig = px.line(final, x=final.index, y="Cumulative_Total_Profit/Loss")

                st.plotly_chart(fig)
                st.dataframe(final)
        with col2:
            st.title('')
            st.text(f"Spread Units: {round(final['Cumulative_Spread_Profit/Loss'].iloc[-1], 2)}")
            st.text(f"ML Units: {round(final['Cumulative_ML_Profit/Loss'].iloc[-1], 2)}")
            st.text(f"Over/Under Units: {round(final['Cumulative_Total_Profit/Loss'].iloc[-1], 2)}")
            st.text(f"Total Units: {round(final['Cumulative_ML_Profit/Loss'].iloc[-1] + final['Cumulative_Spread_Profit/Loss'].iloc[-1] + final['Cumulative_Total_Profit/Loss'].iloc[-1], 2)}")
            st.text('---------------')
            st.text(f"Spread EV: {round(spread_ev, 2)}% in {spread_wins + spread_losses} Games")
            st.text(f"ML EV: {round(ml_ev, 2)}% in {ml_wins + ml_losses} Games")
            st.text(f"Over/Under EV: {round(total_ev, 2)}% in {total_wins + total_losses} Games")
            st.text('---------------')
            st.text(f"Spread Drawdown: {round(final['Spread_Drawdown'].min(), 2)}")
            st.text(f"ML Drawdown: {round(final['ML_Drawdown'].min(), 2)}")
            st.text(f"Over/Under Drawdown: {round(final['Total_Drawdown'].min(), 2)}")
            st.text(f"Max Total Drawdown : {round(final['Combine_Drawdown'].min(), 2)}")
            st.text('---------------')
            st.text(f'Spread Win Percentage: {spread_win_percentage}%')
            st.text(f'ML Win Percentage: {ml_win_percentage}%')
            st.text(f'Over/Under Win Percentage: {total_win_percentage}%')


def live(sport):
    with st.spinner('Wait for it...'):
        with st.sidebar.form('Sportsbook'):
            selected_sportbook = st.selectbox('Select Sportbook', ('Betonline', 'Bovada'))
            run_sportsbook = st.form_submit_button(f'Choose Sportsbook')
        if run_sportsbook:
            data.live_data('2022', sport, selected_sportbook)
        df = model.load_backtest_model(sport, live=True)
        final = pd.DataFrame()
        for index, row in df.iterrows():
            final.loc[index, 'Date'] = row['Date']
            final.loc[index, 'Away_Team'] = row['Away_Team']
            final.loc[index, 'Home_Team'] = row['Home_Team']
            final.loc[index, 'Away_Score'] = round(row['Predicted_Away'])
            final.loc[index, 'Home_Score'] = round(row['Predicted_Home'])

            if row['Predicted_Spread'] < row['Spread'] < 0:
                final.loc[index, 'Spread'] = f'{row["Home_Team"]} {-row["Spread"]}'
            elif row['Spread'] < row['Predicted_Spread'] > 0:
                final.loc[index, 'Spread'] = f'{row["Away_Team"]} {-row["Spread"]}'
            elif row['Predicted_Spread'] < row['Spread'] > 0:
                final.loc[index, 'Spread'] = f'{row["Home_Team"]} {-row["Spread"]}'
            elif row['Predicted_Spread'] > row['Spread'] < 0:
                final.loc[index, 'Spread'] = f'{row["Away_Team"]} {-row["Spread"]}'

            if (sport in leagues['Soccer']) and 0.25 <= row['Predicted_Spread'] <= 0.25:
                final.loc[index, 'MoneyLine'] = f'Draw'
            else:
                if row['Predicted_Spread'] > 0:
                    final.loc[index, 'MoneyLine'] = f'{row["Away_Team"]}'
                else:
                    final.loc[index, 'MoneyLine'] = f'{row["Home_Team"]}'

            if row['Predicted_Total'] > row['Over/Under']:
                final.loc[index, 'Total'] = f'Over {row["Over/Under"]}'
            else:
                final.loc[index, 'Total'] = f'Under {row["Over/Under"]}'

        final.set_index('Date', inplace=True)

        st.dataframe(final)


def manual(sport):
    with st.spinner('Wait for it...'):
        df = data.clean_sr_stats(sport, '2022')
        with st.sidebar.form('Manual'):
            away_team = st.selectbox('Team 1', df['Tm'])
            home_team = st.selectbox('Team 2', df['Tm'])

            run_results = st.form_submit_button(f'Run Results')

        manual_df = df[(df['Tm'] == home_team) | (df['Tm'] == away_team)]
        manual_df.set_index('Tm', inplace=True)

        st.dataframe(manual_df)

        if run_results:
            data_df = data.manual_data('2022', sport, home_team, away_team)
            backtest_df = model.load_backtest_model(sport, manual=True)
            st.dataframe(backtest_df)


def historical_data(sport):
    with st.spinner('Wait for it...'):
        with st.sidebar.form('Refresh Tables'):
            refresh_data = st.form_submit_button('Refresh Data')

        if refresh_data:
            data.refresh(sport)

        with st.sidebar.form('Data'):
            stats_box = st.checkbox('Display Stats Data')
            games_box = st.checkbox('Display Game Data')
            season = st.text_input('Year')
            run_results = st.form_submit_button(f'Run Results')

        if run_results:
            stats_df = data.clean_sr_stats(sport, season)
            games_df = data.clean_odds(sport, season)
            combined_df = data.combine_odds_stats(season, sport)
            st.write(f'Match Rate: {(len(combined_df)/len(games_df))*100}%')
            if stats_box:
                st.write('Stats Data')
                st.dataframe(stats_df)
            if games_box:
                st.write(f'{sport} Game Data')
                st.dataframe(games_df)
            st.write('Combined Data')
            st.dataframe(combined_df)


if select_run_script == 'Backtest':
    backtest(selected_league)
elif select_run_script == 'Live':
    live(selected_league)
elif select_run_script == 'Manual':
    manual(selected_league)
elif select_run_script == 'Data':
    historical_data(selected_league)
