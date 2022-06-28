import pprint

pp = pprint.PrettyPrinter(width=41, compact=True)


class Index:
    def __init__(self, **kwargs):

        data = {'Stats': {'sr_xpaths': [],
                          'sr_url': '',
                          'sr_multi_page': {}
                          },
                'Results': {'sr_xpaths': [],
                            'sr_url': '',
                            'sr_multipage': {},
                            'odds_portal_url': ''
                            },
                'Lines': {'Betonline': '',
                          'Bovada': ''
                          },
                'Conversions': {'Betonline': '',
                                'Bovada': '',
                                'Odds': ''
                                },
                }

        self.season = kwargs.get('season', '2022')
        self.countries = ['usa', 'europe', 'spain', 'italy']
        self.sports = ['Baseball', 'Basketball', 'Football', 'Soccer', 'Tennis']
        self.baseball_leagues = ['MLB', 'MexicanLMB']
        self.basketball_leagues = ['NBA', 'WNBA']
        self.football_leagues = ['NFL', 'CFL']
        self.soccer_leagues = ['EPL', 'MLS']
        self.tennis_leagues = ['ATP', 'WTA']

        baseball_dict = dict.fromkeys(self.baseball_leagues, data)
        basketball_dict = dict.fromkeys(self.basketball_leagues, data)
        football_dict = dict.fromkeys(self.football_leagues, data)
        soccer_dict = dict.fromkeys(self.soccer_leagues, data)
        tennis_dict = dict.fromkeys(self.tennis_leagues, data)

        self.leagues = [baseball_dict, basketball_dict, football_dict, soccer_dict, tennis_dict]
        self.baseball_sr_xpaths = ['//*[@id="teams_standard_batting"]', '//*[@id="teams_standard_pitching"]']

        self.output = dict(zip(self.sports, self.leagues))

        usa_leagues = ['MLB', 'NFL']
        euro_leagues = ['EPL', 'ATP']

        for sport in self.output:
            for league in self.output[sport]:

                if league in usa_leagues:
                    self.output[sport][league]['Lines']['Betonline'] = f'https://www.betonline.ag/sportsbook/{sport}/{league}'.lower()
                    self.output[sport][league]['Results']['odds_portal_url'] = f'https://www.oddsportal.com/{sport}/usa/{league}/results/'.lower()
                elif league in euro_leagues:
                    self.output[sport][league]['Lines']['Betonline'] = f'https://www.betonline.ag/sportsbook/{sport}/europe/{league}'.lower()
                    self.output[sport][league]['Results']['odds_portal_url'] = f'https://www.oddsportal.com/{sport}/europe/{league}/results/'.lower()

            pp.pprint(self.output)


test = Index()
# pp.pprint(test.output)
