PROJECT_NAME = "Tech-Atlas-AT"
PROJECT_VERSION = "0.1.0"
PROJECT_URL = "https://github.com/tech-atlas-master-thesis"
CONTACT_EMAIL = "fabian@mooslechner.dev"

USER_AGENT = f"{PROJECT_NAME}/{PROJECT_VERSION} (+{PROJECT_URL}; {CONTACT_EMAIL})"

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "Accept-Language": "de-AT,de;q=0.9,en;q=0.8",
}

HTML_HEADERS = {
    **DEFAULT_HEADERS,
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
}
