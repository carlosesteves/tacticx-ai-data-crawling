from requests import Session
import requests
from models.match import Match
from pages.league_page_matches import LeaguePageMatches
from pages.match_page import MatchPage
from pipelines.coach_pipeline import run_coach_pipeline
from pipelines.match_pipeline import run_match_pipeline
from repositories.coach.supabase_coach_repository import SupabaseCoachRepository
from repositories.match.supabase_match_repository import SupabaseMatchRepository
from repositories.tenure.supabase_coach_tenure_repository import SupabaseCoachTenureRepository
from repositories.pipeline_context import PipelineContext
from repositories.tenure import supabase_coach_tenure_repository
from services.supabase_service import create_supabase_client
from utils.page_utils import get_points_from_score


def run_season_pipeline(league_id: int, league_code: str, season_id: int, session: Session) -> list:
    err_match_ids = []
    client = create_supabase_client()
    context = PipelineContext(
        coach_repo=SupabaseCoachRepository(client=client),
        match_repo=SupabaseMatchRepository(client=client),
        tenure_repo=SupabaseCoachTenureRepository(client=client),
        coach_cache=set(),
        match_cache=set(),
        tenure_cache=set(),
    )
    
    match_counter = 1
    league_match_ids = LeaguePageMatches(session=session, league_code=league_code, season_id=season_id).get_match_ids()
    for match_id in league_match_ids:
        try:         
            match_counter+=1
            print(f"💬 Processing match={match_id} {match_counter}/{len(league_match_ids)}")   
            run_match_pipeline(
                session=session,
                match_id=match_id,
                league_id=league_id,
                season_id=season_id,
                context=context
            )            
        except Exception as e:
            err_match_ids.append(match_id)
            print(f"❌ Error processing match {match_id}: {e}")

    return err_match_ids


# def run_season_matches(league_id: int, league_code: str, season_id: int, session: Session) -> list:
#     err_match_ids = []
#     client = create_supabase_client()
#     context = PipelineContext(
#         coach_repo=SupabaseCoachRepository(client=client),
#         match_repo=SupabaseMatchRepository(client=client),
#         tenure_repo=SupabaseCoachTenureRepository(client=client),
#         coach_cache=set(),
#         match_cache=set(),
#         tenure_cache=set(),
#     )
    
#     match_counter = 1
#     matches = LeaguePageMatches(session=session, league_code=league_code, season_id=season_id).get_matches()
#     for match in matches:        
#         match_id = int(match['match_id'])              
#         print(f"Processing league_id={league_id} season={season_id} match={match_id}")
#         print(context.match_repo.does_match_exist(match_id))
#         if not context.match_repo.does_match_exist(match_id):
#             print(f"Match {match_id} doesn't exist!")
#             date = match['date']
#             home_club_id = int(match['home_club_id'])
#             away_club_id = int(match['away_club_id'])
#             home_coach = context.coach_repo.get_coach_id_by_date(club_id=home_club_id, match_date=date)
#             away_coach = context.coach_repo.get_coach_id_by_date(club_id=away_club_id, match_date=date)
#             home_goals = match['home_goals']
#             away_goals = match['away_goals']  

#             if not home_coach or not away_coach:
#                 print(f"Running page for match {match_id}")
#                 run_match_pipeline(
#                     session=session,
#                     match_id=match_id,
#                     league_id=league_id,
#                     season_id=season_id,
#                     context=context
#                 )   
#             else:
#                 print(f"Saving without page for match {match_id}")
#                 match = Match(
#                     tm_match_id=int(match_id),
#                     home_club_id=int(home_club_id),
#                     away_club_id=int(away_club_id),
#                     season_id=season_id,
#                     league_id=league_id,
#                     date=date,
#                     home_coach_id=int(home_coach[0]['coach_id']),
#                     away_coach_id=int(away_coach[0]['coach_id']),
#                     attendance=0,
#                     home_team_score=home_goals,
#                     away_team_score=away_goals,
#                     home_team_points=get_points_from_score(f"{home_goals}:{away_goals}")[0],
#                     away_team_points=get_points_from_score(f"{home_goals}:{away_goals}")[1]
#                 )
#                 context.match_repo.save(match)
        

#             # try:         
#     #         match_counter+=1
#     #         print(f"💬 Processing match={match_id} {match_counter}/{len(league_match_ids)}")   
#             # run_match_pipeline(
#     #             session=session,
#     #             match_id=match_id,
#     #             league_id=league_id,
#     #             season_id=season_id,
#     #             context=context
#     #         )            
#     #     except Exception as e:
#     #         err_match_ids.append(match_id)
#     #         print(f"❌ Error processing match {match_id}: {e}")
#             # context.match_repo.save(match)
#             # context.match_cache.add(match.tm_match_id)
    
#     # for match_id in league_match_ids:
#     #     try:         
#     #         match_counter+=1
#     #         print(f"💬 Processing match={match_id} {match_counter}/{len(league_match_ids)}")   
#     #         run_match_pipeline(
#     #             session=session,
#     #             match_id=match_id,
#     #             league_id=league_id,
#     #             season_id=season_id,
#     #             context=context
#     #         )            
#     #     except Exception as e:
#     #         err_match_ids.append(match_id)
#     #         print(f"❌ Error processing match {match_id}: {e}")

#     # return err_match_ids