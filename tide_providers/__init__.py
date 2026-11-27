"""Official national tide predictions for the Tide layer (plan section 112, step 3). One module per agency.

Every agency module defines exactly this interface (tide_global.py does the rest: the 30-min grid, the window,
caching, the sky, the payload):

    PREFIX: str        station id prefix, e.g. "chs" (ids are "<PREFIX>:<local id>", lowercase [a-z0-9._-], <= 80)
    NAME: str          the agency's short name for the window, e.g. "Canadian Hydrographic Service"
    CREDIT: str        the credit line the agency's terms ask for (plain text, one sentence or two)
    DATUM: str         how heights are referenced, shown in the window, e.g. "chart datum (= LAT)"
    MAX_REQUESTS: int  upper bound of upstream requests one fetch() makes

    def build_station_list(get) -> list[dict]
        Offline (tools/tides/build_national.py): every station worth showing, as dicts
        {"id": "<PREFIX>:<local>", "name": str, "lat": float, "lon": float, "local": {...}} where "local" holds what
        fetch() needs (agency codes, file names, a datum offset ...). JSON-serialisable. Stations without a position
        or without predictions are left out.

    def page_url(st) -> str | None
        The agency's own public page for this station (the window links to it), or None.

    def fetch(st, begin, end, get) -> {"curve": [(epoch_s, metres), ...], "hilo": [(epoch_s, metres, "H" | "L"), ...]}
        Live (the site, at most once a station per UTC day): the agency's predictions covering [begin, end] (epoch
        seconds, UTC; about 33 days), heights in metres above DATUM. "curve": any regular step up to 60 minutes, or []
        when the agency publishes only highs and lows. Raise TideFetchError for any failure or an answer that cannot be
        read (never return partial garbage).

`get(url, headers=None, data=None)` is passed in by the caller: it returns the body as bytes for HTTP 200 and raises
on anything else (timeouts, other statuses, oversize). data: bytes for a POST. Modules never import requests or touch
the network themselves, so tests feed fixtures through a fake get.
"""


class TideFetchError(Exception):
    pass
