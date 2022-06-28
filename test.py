from Scraping.cleaning_data import CleanData
from Scraping.scraping import Scraper
from Scraping.model import Model

data = CleanData()
scraper = Scraper()
model = Model()

# seasons = ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022']
# for season in seasons:
    # data.clean_international_basketball(season, 'EuroLeague')

# data.combine_seasons('EuroLeague')

# data.clean_international_basketball('2020', 'EuroLeague')

# print(scraper.get_betonline_lines('EuroLeague'))

# data.clean_international_basketball('2020', 'EuroLeague')

# data.clean_sr_stats('EPL', '2021', rerun=False)
# data.clean_odds('EPL', '2021')
# data.combine_odds_stats('2021', 'EPL')

# data.combine_seasons('MLB')

# data.live_data('2022', 'ChinaSuperLeague', 'Betonline')


def run_new(sport):
    data.refresh(sport)
    data.combine_seasons(sport)

    model.build_model(sport)
    model.load_backtest_model(sport)
    model.backtest_stats(sport)


def test_name_matches(sport, season):
    data.clean_sr_stats(sport, season, rerun=False)
    data.clean_odds(sport, season, rerun=False)
    data.combine_odds_stats(season, sport)

# scraper.odds_portal_scrape('CPBL', '2021')
# data.clean_odds('NPB', '2021', rerun=False)
# data.clean_sr_stats('SwedenSuperettan', '2021', rerun=False)
# print(scraper.multi_page_dict.keys())


run_new('ParaguayPrimeraDivision')
