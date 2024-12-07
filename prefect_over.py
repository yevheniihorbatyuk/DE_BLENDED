import httpx


def show_stars(github_repos: list[str]):
    """Show the number of stars that GitHub repos have"""

    for repo in github_repos:
        repo_stats = fetch_stats(repo)
        stars = get_stars(repo_stats)
        print(f"{repo}: {stars} stars")


def fetch_stats(github_repo: str):
    """Fetch the statistics for a GitHub repo"""

    return httpx.get(f"https://api.github.com/repos/{github_repo}").json()


def get_stars(repo_stats: dict):
    """Get the number of stars from GitHub repo statistics"""

    return repo_stats['stargazers_count']


if __name__ == "__main__":
    show_stars([
        "PrefectHQ/prefect",
        "pydantic/pydantic",
        "huggingface/transformers"
    ])
