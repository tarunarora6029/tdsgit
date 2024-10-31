import requests
import pandas as pd
import time
import logging
from typing import List, Dict, Optional, Any
from requests.exceptions import RequestException
from datetime import datetime
from ratelimit import limits, sleep_and_retry  # New import for rate limiting
import json

class GitHubScraper:
    def __init__(self, token: str, location: str, min_followers: int):
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }
        self.base_url = "https://api.github.com"
        self.location = location
        self.min_followers = min_followers
        self.setup_logging()
        # Add rate limit tracking
        self.rate_limit_remaining = float('inf')
        self.rate_limit_reset = 0

    def setup_logging(self):
        """Enhanced logging setup with file handler"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'github_scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )

    def update_rate_limit_info(self, response: requests.Response) -> None:
        """Track GitHub API rate limit information"""
        self.rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
        self.rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', 0))
        logging.info(f"Rate limit remaining: {self.rate_limit_remaining}")

    @sleep_and_retry
    @limits(calls=30, period=60)  # Rate limit to 30 calls per minute
    def make_request(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Enhanced request method with better error handling and rate limiting"""
        max_retries = 3
        wait_time = 1  # Initial wait time in seconds

        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=10)
                self.update_rate_limit_info(response)

                if response.status_code == 403:
                    if 'rate limit exceeded' in response.text.lower():
                        sleep_time = max(self.rate_limit_reset - time.time(), 0)
                        logging.warning(f"Rate limit exceeded. Sleeping for {sleep_time} seconds")
                        time.sleep(sleep_time + 1)  # Add 1 second buffer
                        continue

                if response.status_code == 202:
                    # Handle GitHub's acceptance but not ready response
                    logging.info("GitHub is processing the request. Waiting...")
                    time.sleep(2)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                logging.warning(f"Timeout on attempt {attempt + 1} of {max_retries}")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff

            except requests.exceptions.RequestException as e:
                logging.error(f"Request failed on attempt {attempt + 1}: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff

        return {}

    def clean_company_name(self, company: Optional[str]) -> str:
        """Enhanced company name cleaning with additional rules"""
        if not company:
            return ""
        # Remove common prefixes/suffixes and standardize
        company = company.strip().upper()
        prefixes_to_remove = ['@', 'HTTP://', 'HTTPS://', 'WWW.']
        for prefix in prefixes_to_remove:
            if company.startswith(prefix):
                company = company[len(prefix):]
        # Remove common domain extensions
        domains = ['.COM', '.ORG', '.NET', '.CO', '.IO']
        for domain in domains:
            if company.endswith(domain):
                company = company[:-len(domain)]
        return company.strip()

    def fetch_users(self) -> List[Dict]:
        """Enhanced user fetching with progress tracking"""
        users = []
        page = 1
        total_count = 0
        
        while True:
            logging.info(f"Fetching users page {page}")
            params = {
                "q": f"location:{self.location} followers:>{self.min_followers}",
                "per_page": 100,
                "page": page
            }
            
            data = self.make_request(f"{self.base_url}/search/users", params)
            
            if not data.get("items"):
                break
                
            current_items = data["items"]
            users.extend(current_items)
            
            # Update total count and progress
            if page == 1:
                total_count = min(data.get("total_count", 0), 1000)  # GitHub API limit
            
            logging.info(f"Progress: {len(users)}/{total_count} users fetched")
            
            if len(current_items) < 100 or len(users) >= total_count:
                break
                
            page += 1
            
        return users

    def fetch_user_details(self, username: str) -> Dict:
        """Enhanced user details fetching with better error handling"""
        logging.info(f"Fetching details for user {username}")
        user_data = self.make_request(f"{self.base_url}/users/{username}")
        
        # Enhanced data validation
        return {
            'login': user_data.get('login', ''),
            'name': user_data.get('name', ''),
            'company': self.clean_company_name(user_data.get('company')),
            'location': user_data.get('location', ''),
            'email': user_data.get('email', ''),
            'hireable': bool(user_data.get('hireable')),  # Ensure boolean
            'bio': user_data.get('bio', ''),
            'public_repos': int(user_data.get('public_repos', 0)),  # Ensure integer
            'followers': int(user_data.get('followers', 0)),
            'following': int(user_data.get('following', 0)),
            'created_at': user_data.get('created_at', '')
        }

    def fetch_repositories(self, username: str) -> List[Dict]:
        """Enhanced repository fetching with better data handling"""
        repos = []
        page = 1
        
        while page <= 5:  # Limit to 500 repos (100 per page * 5 pages)
            logging.info(f"Fetching repositories page {page} for user {username}")
            params = {
                "per_page": 100,
                "page": page,
                "sort": "pushed",
                "direction": "desc"
            }
            
            data = self.make_request(f"{self.base_url}/users/{username}/repos", params)
            
            if not data:
                break
                
            for repo in data:
                # Enhanced data validation and cleaning
                license_info = repo.get('license') or {}
                repo_data = {
                    'login': username,
                    'full_name': repo.get('full_name', ''),
                    'created_at': repo.get('created_at', ''),
                    'stargazers_count': int(repo.get('stargazers_count', 0)),
                    'watchers_count': int(repo.get('watchers_count', 0)),
                    'language': repo.get('language', ''),
                    'has_projects': bool(repo.get('has_projects', False)),
                    'has_wiki': bool(repo.get('has_wiki', False)),
                    'license_name': license_info.get('key', '')
                }
                repos.append(repo_data)
            
            if len(data) < 100:
                break
                
            page += 1
            
        return repos

    def analyze_data(self, users_data: List[Dict], repos_data: List[Dict]) -> Dict:
        """New method to analyze collected data"""
        analysis = {
            'total_users': len(users_data),
            'total_repos': len(repos_data),
            'hireable_users': sum(1 for user in users_data if user['hireable']),
            'languages': {},
            'avg_stars_per_repo': 0,
            'most_active_user': '',
            'most_starred_repo': ''
        }
        
        # Analyze languages
        for repo in repos_data:
            lang = repo['language'] or 'Unknown'
            analysis['languages'][lang] = analysis['languages'].get(lang, 0) + 1
            
        # Calculate averages and find top contributors
        if repos_data:
            analysis['avg_stars_per_repo'] = sum(repo['stargazers_count'] for repo in repos_data) / len(repos_data)
            
        # Find most active user and most starred repo
        user_repo_counts = {}
        most_stars = 0
        for repo in repos_data:
            user_repo_counts[repo['login']] = user_repo_counts.get(repo['login'], 0) + 1
            if repo['stargazers_count'] > most_stars:
                most_stars = repo['stargazers_count']
                analysis['most_starred_repo'] = repo['full_name']
                
        if user_repo_counts:
            analysis['most_active_user'] = max(user_repo_counts.items(), key=lambda x: x[1])[0]
            
        return analysis

    def save_data(self, users_data: List[Dict], repos_data: List[Dict]):
        """Enhanced data saving with analysis"""
        # Analyze data
        analysis_results = self.analyze_data(users_data, repos_data)
        
        # Save data to CSV
        pd.DataFrame(users_data).to_csv("users.csv", index=False)
        pd.DataFrame(repos_data).to_csv("repositories.csv", index=False)
        
        # Create README with analysis insights
        self.create_readme(analysis_results)
        
        # Save analysis results
        with open('analysis_results.json', 'w') as f:
            json.dump(analysis_results, f, indent=2)

    def create_readme(self, analysis: Dict):
        """Enhanced README creation with actual data insights"""
        readme_content = f"""
# GitHub Users and Repositories Analysis for {self.location}

- Data was scraped using GitHub's API with rate limiting and error handling, processing {analysis['total_users']} users and {analysis['total_repos']} repositories.
- Analysis reveals {analysis['languages'].get('Python', 0)} Python repositories and {analysis['hireable_users']} developers open to job opportunities.
- Developers should focus on {max(analysis['languages'].items(), key=lambda x: x[1])[0]} projects, as it's the most popular language in the community.

## Files
- `users.csv`: Information about GitHub users in {self.location} with {self.min_followers}+ followers
- `repositories.csv`: Details of public repositories for these users
- `github_scraper.py`: Source code for data collection
- `analysis_results.json`: Detailed analysis of the collected data

## Key Statistics
- Total Users: {analysis['total_users']}
- Total Repositories: {analysis['total_repos']}
- Average Stars per Repository: {analysis['avg_stars_per_repo']:.2f}
- Most Active User: {analysis['most_active_user']}
- Most Starred Repository: {analysis['most_starred_repo']}

## Top Programming Languages
"""
        # Add language statistics
        for lang, count in sorted(analysis['languages'].items(), key=lambda x: x[1], reverse=True)[:5]:
            readme_content += f"- {lang}: {count} repositories\n"
            
        with open("README.md", "w") as f:
            f.write(readme_content)

def main():
    scraper = GitHubScraper(
        token="",
        location="Sydney",
        min_followers=100
    )
    
    try:
        users = scraper.fetch_users()
        logging.info(f"Found {len(users)} users")
        
        users_data = []
        for user in users:
            try:
                user_details = scraper.fetch_user_details(user['login'])
                users_data.append(user_details)
            except Exception as e:
                logging.error(f"Error fetching details for user {user['login']}: {str(e)}")
                continue
        
        repos_data = []
        for user in users_data:
            try:
                user_repos = scraper.fetch_repositories(user['login'])
                repos_data.extend(user_repos)
            except Exception as e:
                logging.error(f"Error fetching repositories for user {user['login']}: {str(e)}")
                continue
        
        scraper.save_data(users_data, repos_data)
        logging.info("Data collection and analysis completed successfully")
        
    except Exception as e:
        logging.error(f"Script failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    main()