import cfbd 
from cfbd.rest import ApiException

import pandas as pd
from tqdm import tqdm


def create_lines_df_from_api_response(api_response):
    final_df = pd.DataFrame(columns=['season','week','id','season_type','start_date','home_team','home_conference',
                                       'home_score','away_team','away_conference','away_score','provider','over_under',
                                       'spread','home_moneyline','away_moneyline','formatted_spread','over_under_open',
                                       'spread_open'])

    pbar = tqdm(total=len(api_response))

    for n in range(0,len(api_response)):
        season = api_response[n].season
        week = api_response[n].week
        season_type = api_response[n].season_type
        start_date = api_response[n].start_date
        _id = api_response[n].id
        home_team = api_response[n].home_team
        home_conference = api_response[n].home_conference
        home_score = api_response[n].home_score
        away_team = api_response[n].away_team
        away_conference = api_response[n].away_conference
        away_score = api_response[n].away_score
        lines = api_response[n].lines

        if len(lines) == 0:
            away_moneyline = ''
            formatted_spread = ''
            home_moneyline = ''
            over_under = ''
            over_under_open = ''
            provider = ''
            spread = ''
            spread_open = ''

            # Make a df w/ this data
            df = pd.DataFrame(data=[season,week,_id,season_type,start_date,home_team,home_conference,
                                    home_score,away_team,away_conference,away_score,provider,over_under,
                                    spread,home_moneyline,away_moneyline,formatted_spread,over_under_open,
                                    spread_open])
            df = df.T

            df.columns = ['season','week','id','season_type','start_date','home_team','home_conference',
                           'home_score','away_team','away_conference','away_score','provider','over_under',
                           'spread','home_moneyline','away_moneyline','formatted_spread','over_under_open',
                           'spread_open']

            final_df = pd.concat([final_df,df]).reset_index().drop('index',axis=1)

        else:
            for i in range(0,len(lines)):
                away_moneyline = lines[i].away_moneyline
                formatted_spread = lines[i].formatted_spread
                home_moneyline = lines[i].home_moneyline
                over_under = lines[i].over_under
                over_under_open = lines[i].over_under_open
                provider = lines[i].provider
                spread = lines[i].spread
                spread_open = lines[i].spread_open

                # Make a df w/ this data 
                df = pd.DataFrame(data=[season,week,_id,season_type,start_date,home_team,home_conference,
                                        home_score,away_team,away_conference,away_score,provider,over_under,
                                        spread,home_moneyline,away_moneyline,formatted_spread,over_under_open,
                                        spread_open])
                df = df.T

                df.columns = ['season','week','id','season_type','start_date','home_team','home_conference',
                           'home_score','away_team','away_conference','away_score','provider','over_under',
                           'spread','home_moneyline','away_moneyline','formatted_spread','over_under_open',
                           'spread_open']

                final_df = pd.concat([final_df,df]).reset_index().drop('index',axis=1)

        pbar.update(1)
    pbar.close()

    final_col_order = ['id','home_team','home_score','away_team','away_score','provider','over_under','spread',
                       'formatted_spread','spread_open','over_under_open','home_moneyline','away_moneyline']

    final_col_aliases = ['Id','HomeTeam','HomeScore','AwayTeam','AwayScore','LineProvider','OverUnder','Spread',
                         'FormattedSpread','OpeningSpread','OpeningOverUnder','HomeMoneyline','AwayMoneyline']

    final_df = final_df[final_col_order]
    final_df.columns = final_col_aliases

    return final_df


def create_talent_df_from_api_response(api_response):

    final_df = pd.DataFrame(columns=['school','talent','year'])

    pbar = tqdm(total=len(api_response))

    for i in range(0,len(api_response)):

        school = api_response[i].school
        talent = api_response[i].talent
        year = api_response[i].year

        df = pd.DataFrame(data=[school, talent, year])
        df = df.T
        df.columns = ['school','talent','year']

        final_df = pd.concat([final_df, df]).reset_index().drop('index',axis=1)

        pbar.update(1)
    pbar.close()


    final_col_order = ['year','school','talent']

    final_col_aliases = ['Year','School','Talent']

    final_df = final_df[final_col_order]
    final_df.columns = final_col_aliases

    return final_df


def create_ppa_df_from_api_response(api_response):
    final_df = pd.DataFrame(columns = ['game_id','season','week','conference','team','opponent','off_overall','off_passing','off_rushing',
                                      'off_firstdown','off_seconddown','off_thirddown','def_overall','def_passing','def_rushing',
                                      'def_firstdown','def_seconddown','def_thirddown'])

    pbar = tqdm(total=len(api_response))

    for i in range(0, len(api_response)):

        game_id = api_response[i].game_id
        season = api_response[i].season
        week = api_response[i].week
        conference = api_response[i].conference
        team = api_response[i].team
        opponent = api_response[i].opponent

        defense = api_response[i].defense
        offense = api_response[i].offense

        def_overall = defense.overall
        def_passing = defense.passing
        def_rushing = defense.rushing
        def_firstdown = defense.first_down
        def_seconddown = defense.second_down
        def_thirddown = defense.third_down

        off_overall = offense.overall
        off_passing = offense.passing
        off_rushing = offense.rushing
        off_firstdown = offense.first_down
        off_seconddown = offense.second_down
        off_thirddown = offense.third_down

        df = pd.DataFrame(data=[game_id,season,week,conference,team,opponent,off_overall,off_passing,off_rushing,
                              off_firstdown,off_seconddown,off_thirddown,def_overall,def_passing,def_rushing,
                              def_firstdown,def_seconddown,def_thirddown])
        df = df.T
        df.columns = ['game_id','season','week','conference','team','opponent','off_overall','off_passing','off_rushing',
                      'off_firstdown','off_seconddown','off_thirddown','def_overall','def_passing','def_rushing',
                      'def_firstdown','def_seconddown','def_thirddown']

        final_df = pd.concat([final_df,df]).reset_index().drop('index',axis=1)

        pbar.update(1)
    pbar.close()

    final_col_order = ['game_id','season','week','conference','team','opponent','off_overall','off_passing','off_rushing',
                      'off_firstdown','off_seconddown','off_thirddown','def_overall','def_passing','def_rushing',
                      'def_firstdown','def_seconddown','def_thirddown']

    final_col_aliases = ['GameId','Season','Week','Conference','Team','Opponent','Offense Overall','Offense Passing',
                         'Offense Rushing','Offense FirstDown','Offense SecondDown','Offense ThirdDown','Defense Overall',
                         'Defense Passing','Defense Rushing','Defense FirstDown','Defense SecondDown','Defense ThirdDown']

    final_df = final_df[final_col_order]
    final_df.columns = final_col_aliases

    return final_df


def create_rank_df_from_api_response(api_response):
    final_df = pd.DataFrame(columns = ['season','season_type','week','poll','school','conference','rank','points','first_place_votes'])

    pbar = tqdm(total=len(api_response))
    for i in range(0,len(api_response)):
        season = api_response[i].season
        season_type = api_response[i].season_type
        week = api_response[i].week

        polls = api_response[i].polls

        for n in range(0,len(polls)):
            poll = polls[n].poll

            ranks = polls[n].ranks
            for k in range(0,len(ranks)):
                school = ranks[k].school
                conference = ranks[k].conference
                rank = ranks[k].rank
                points = ranks[k].points
                first_place_votes = ranks[k].first_place_votes

                # Add row to df here
                df = pd.DataFrame(data=[season, season_type, week, poll, school, conference, rank,points,first_place_votes])
                df = df.T
                df.columns = ['season','season_type','week','poll','school','conference','rank','points','first_place_votes']

                final_df = pd.concat([final_df,df]).reset_index().drop('index',axis=1)

        pbar.update(1)
    pbar.close()

    final_col_order = ['season','season_type','week','poll','rank','school','conference','first_place_votes','points']

    final_col_aliases = ['Season','SeasonType','Week','Poll','Rank','School','Conference','FirstPlaceVotes','Points']

    final_df = final_df[final_col_order]
    final_df.columns = final_col_aliases

    return final_df


def create_schedule_df_from_api_response(api_response):

    final_df = pd.DataFrame(columns=['id','season','week','season_type','start_date','start_time_tbd','completed','neutral_site','conference_game',
                                    'attendance','venue_id','venue','home_id','home_team','home_conference','home_division','home_points', 
                                    'home_line_scores','home_post_win_prob','home_pregame_elo','home_postgame_elo', 'away_id','away_team',
                                    'away_conference','away_division','away_points','away_line_scores','away_post_win_prob','away_pregame_elo',
                                    'away_postgame_elo','excitement_index','highlights','notes'])

    pbar = tqdm(total = len(api_response))

    for i in range(0,len(api_response)):
        _id = api_response[i].id
        season = api_response[i].season
        week = api_response[i].week
        season_type = api_response[i].season_type
        start_date = api_response[i].start_date
        start_time_tbd = api_response[i].start_time_tbd
        completed = api_response[i].completed
        neutral_site = api_response[i].neutral_site
        conference_game = api_response[i].conference_game
        attendance = api_response[i].attendance
        venue_id = api_response[i].venue_id
        venue = api_response[i].venue
        home_id = api_response[i].home_id
        home_team = api_response[i].home_team
        home_conference = api_response[i].home_conference
        home_division = api_response[i].home_division
        home_points = api_response[i].home_points
        home_line_scores = api_response[i]. home_line_scores
        home_post_win_prob = api_response[i].home_post_win_prob
        home_pregame_elo = api_response[i].home_pregame_elo 
        home_postgame_elo = api_response[i].home_postgame_elo
        away_id = api_response[i].away_id
        away_team = api_response[i].away_team
        away_conference = api_response[i].away_conference
        away_division = api_response[i].away_division
        away_points = api_response[i].away_points
        away_line_scores = api_response[i].away_line_scores
        away_post_win_prob = api_response[i].away_post_win_prob
        away_pregame_elo = api_response[i].away_pregame_elo
        away_postgame_elo = api_response[i].away_postgame_elo
        excitement_index = api_response[i].excitement_index
        highlights = api_response[i].highlights
        notes = api_response[i].notes

        df = pd.DataFrame(data=[_id, season, week, season_type, start_date, start_time_tbd, completed, neutral_site, conference_game,
                                attendance, venue_id, venue, home_id, home_team, home_conference, home_division, home_points, 
                                home_line_scores, home_post_win_prob, home_pregame_elo, home_postgame_elo, away_id, away_team,
                                away_conference, away_division, away_points, away_line_scores, away_post_win_prob, away_pregame_elo,
                                away_postgame_elo, excitement_index, highlights, notes])
        df = df.T
        df.columns = ['id','season','week','season_type','start_date','start_time_tbd','completed','neutral_site','conference_game',
                    'attendance','venue_id','venue','home_id','home_team','home_conference','home_division','home_points', 
                    'home_line_scores','home_post_win_prob','home_pregame_elo','home_postgame_elo', 'away_id','away_team',
                    'away_conference','away_division','away_points','away_line_scores','away_post_win_prob','away_pregame_elo',
                    'away_postgame_elo','excitement_index','highlights','notes']

        final_df = pd.concat([final_df,df]).reset_index().drop('index',axis=1)

        pbar.update(1)
    pbar.close()

    final_col_order = ['id','season','week','season_type','start_date','start_time_tbd','completed','neutral_site','conference_game',
                        'attendance','venue_id','venue','home_id','home_team','home_conference','home_division','home_points', 
                        'home_line_scores','home_post_win_prob','home_pregame_elo','home_postgame_elo', 'away_id','away_team',
                        'away_conference','away_division','away_points','away_line_scores','away_post_win_prob','away_pregame_elo',
                        'away_postgame_elo','excitement_index','highlights','notes']

    final_col_aliases = ['Id','Season','Week','Season Type','Start Date','Start Time Tbd','Completed','Neutral Site',
                         'Conference Game','Attendance','Venue Id','Venue','Home Id','Home Team','Home Conference',
                         'Home Division','Home Points','Home Line Scores','Home Post Win Prob','Home Pregame Elo',
                         'Home Postgame Elo','Away Id','Away Team','Away Conference','Away Division','Away Points','Away Line Scores',
                         'Away Post Win Prob','Away Pregame Elo','Away Postgame Elo','Excitement Index','Highlights',
                         'Notes']

    final_df = final_df[final_col_order]
    final_df.columns = final_col_aliases

    return final_df


def main_cfbd_api_call(year):
    # Configure API key authorization: ApiKeyAuth
    configuration = cfbd.Configuration()
    configuration.api_key['Authorization'] = 'ZbqYyKtTEE1O+7uSvGoIl8eD9Y4W3PNFHAujy5kcW7pjGTl5nRDEHmP6e3SdWKx4'
    configuration.api_key_prefix['Authorization'] = 'Bearer'
    ###########################################################################
    # Lines
    api_instance = cfbd.BettingApi(cfbd.ApiClient(configuration))

    try:
        api_response = api_instance.get_lines(year=year)
    except ApiException as e:
        print("Exception when calling BettingApi->get_lines: %s\n" % e)


    lines_df = create_lines_df_from_api_response(api_response)
    ###########################################################################
    # Talent
    api_instance = cfbd.TeamsApi(cfbd.ApiClient(configuration))

    try:
        # Team talent composite rankings
        api_response = api_instance.get_talent(year=year)
    except ApiException as e:
        print("Exception when calling TeamsApi->get_talent: %s\n" % e)

    talent_df = create_talent_df_from_api_response(api_response)
    ###########################################################################
    # PPA
    api_instance = cfbd.MetricsApi(cfbd.ApiClient(configuration))

    try:
        # Team talent composite rankings
        api_response = api_instance.get_game_ppa(year=year, exclude_garbage_time = True)
    except ApiException as e:
        print("Exception when calling TeamsApi->get_game_ppa: %s\n" % e)

    ppa_df = create_ppa_df_from_api_response(api_response)
    ###########################################################################
    # Ranks
    api_instance = cfbd.RankingsApi(cfbd.ApiClient(configuration))

    try:
        # Team talent composite rankings
        api_response = api_instance.get_rankings(year=year)
    except ApiException as e:
        print("Exception when calling TeamsApi->get_rankings: %s\n" % e)

    rank_df = create_rank_df_from_api_response(api_response)
    ###########################################################################
    # Schedule
    api_instance = cfbd.GamesApi(cfbd.ApiClient(configuration))

    try:
        # Team talent composite rankings
        api_response = api_instance.get_games(year=year)
    except ApiException as e:
        print("Exception when calling TeamsApi->get_games: %s\n" % e)

    schedule_df = create_schedule_df_from_api_response(api_response)
    ###########################################################################
#     schedule_df.to_csv(f'./data/processed/{year}_cfb_schedule.csv')
#     rank_df.to_csv(f'./data/processed/{year}_cfb_ap_poll.csv')
#     ppa_df.to_csv(f'./data/processed/{year}_ppa_by_week_data.csv')
#     talent_df.to_csv(f'./data/processed/{year}_cfb_talent_data.csv')
#     lines_df.to_csv(f'./data/processed/{year}_cfb_lines_data.csv')
    
    return schedule_df, rank_df, ppa_df, talent_df, lines_df