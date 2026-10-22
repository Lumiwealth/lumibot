"""Explicit, owner-authorized Community posting with a dedicated scoped key."""
from urllib.parse import urlparse
from uuid import UUID
import requests


class CommunityClient:
    def __init__(self, *, api_key: str, api_url: str = 'https://api.botspot.trade'):
        if not api_key or not isinstance(api_key, str):
            raise ValueError('A dedicated Community API key is required.')
        parsed = urlparse(api_url)
        if parsed.scheme != 'https' and not (parsed.scheme == 'http' and parsed.hostname in {'localhost', '127.0.0.1'}):
            raise ValueError('Use HTTPS, or an explicit local development URL.')
        self._api_key = api_key
        self._api_url = api_url.rstrip('/')

    def post(self, body: str, *, marketplace_listing_id: str | None = None,
             publication_id: str | None = None, parent_post_id: str | None = None,
             verified_trade_source: dict | None = None, request_id: str | None = None) -> dict:
        """Publish once; optionally preserve a caller's UUID across explicit retries.

        Reuse the UUID only for the same post or reply. The server rechecks
        current permission and visibility before returning the original post.
        Transport errors remain visible and are never retried automatically.
        """
        if not isinstance(body, str) or not body.strip() or len(body) > 100_000 or len(body.encode('utf-8')) > 400 * 1024:
            raise ValueError('body must contain 1–100,000 characters and fit within 400 KiB.')
        if request_id is not None:
            try:
                if not isinstance(request_id, str) or str(UUID(request_id)) != request_id.lower():
                    raise ValueError
            except ValueError:
                raise ValueError('request_id must be a hyphenated UUID string.') from None
        payload = {'body': body}
        for key, value in [('marketplaceListingId', marketplace_listing_id),
                           ('publicationId', publication_id), ('parentPostId', parent_post_id),
                           ('verifiedTradeSource', verified_trade_source), ('requestId', request_id)]:
            if value is not None: payload[key] = value
        response = requests.post(self._api_url + '/community/agent/posts',
                                 json=payload, timeout=15,
                                 headers={'Authorization': 'Bearer ' + self._api_key})
        response.raise_for_status()
        return response.json()['post']
