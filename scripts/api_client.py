import requests
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FootballAPIClient:

    BASE_URL = "https://api.football-data.org/v4"
    
    def __init__(self):
        self.api_key = os.getenv('FOOTBALL_API_KEY')
        if not self.api_key:
            raise ValueError("FOOTBALL_API_KEY not found in environment variables")
        self.headers = {"X-Auth-Token": self.api_key}
        logger.info("FootballAPIClient initialized")
    
    def _make_request(self, endpoint: str) -> dict:
        url = f"{self.BASE_URL}/{endpoint}"
        logger.info(f"Making request to: {url}")
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            logger.info(f"Request successful: {response.status_code}")
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error occurred: {e}")
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout error occurred: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error occurred: {e}")
            raise
    
    def get_competitions(self) -> dict:
        return self._make_request("competitions")
    
    def get_competition(self, competition_code: str) -> dict:
        return self._make_request(f"competitions/{competition_code}")
    
    def get_matches(self, competition_code: str = "PL", season: int = None) -> dict:

        endpoint = f"competitions/{competition_code}/matches"
        if season:
            endpoint += f"?season={season}"
        return self._make_request(endpoint)
    
    def get_standings(self, competition_code: str = "PL") -> dict:
        """Get current standings for a competition"""
        return self._make_request(f"competitions/{competition_code}/standings")
    
    def get_teams(self, competition_code: str = "PL") -> dict:
        """Get all teams in a competition"""
        return self._make_request(f"competitions/{competition_code}/teams")
    
    def get_team(self, team_id: int) -> dict:
        """Get details for a specific team"""
        return self._make_request(f"teams/{team_id}")
    
    def get_scorers(self, competition_code: str = "PL") -> dict:
        """Get top scorers for a competition"""
        return self._make_request(f"competitions/{competition_code}/scorers")


# Test the client
if __name__ == "__main__":
    client = FootballAPIClient()
    
    # Test: Get Premier League standings
    print("\n=== Testing API Client ===")
    standings = client.get_standings("PL")
    print(f"\nCompetition: {standings['competition']['name']}")
    print(f"Season: {standings['season']['startDate']} to {standings['season']['endDate']}")
    print("\nTop 5 Teams:")
    for team in standings['standings'][0]['table'][:5]:
        print(f"  {team['position']}. {team['team']['name']} - {team['points']} pts")