"""
Apify token accessor. Kept minimal to match the handoff import contract.
"""
import os

# workers/common.py loads .env on import, but this module may be imported
# before common, so we do a lazy read via os.getenv at call time.
APIFY_TOKEN: str = os.getenv("APIFY_API_TOKEN", "")
