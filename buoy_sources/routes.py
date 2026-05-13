from __future__ import annotations

from flask import jsonify, request

from .registry import get_all_live_buoys, get_one_live_buoy


def register_live_buoy_routes(app):
    @app.route("/api/live-buoys/global")
    def api_live_buoys_global():
        """
        Return latest normalized live observations from all enabled public sources.

        Optional query params:
          sources=ndbc,cdip,ireland,canada
          max_age_hours=96
        """
        sources_raw = request.args.get("sources", "").strip()
        enabled_sources = None

        if sources_raw:
            enabled_sources = {
                part.strip().lower()
                for part in sources_raw.split(",")
                if part.strip()
            }

        max_age_hours = request.args.get("max_age_hours", "96").strip()

        try:
            max_age_hours_value = float(max_age_hours)
        except Exception:
            max_age_hours_value = 96.0

        return jsonify(
            get_all_live_buoys(
                enabled_sources=enabled_sources,
                max_age_hours=max_age_hours_value,
            )
        )

    @app.route("/api/live-buoys/<source_key>/<station_id>")
    def api_live_buoy_detail(source_key, station_id):
        buoy = get_one_live_buoy(source_key, station_id)

        if buoy is None:
            return jsonify({"error": "Buoy not found", "source_key": source_key, "station_id": station_id}), 404

        return jsonify(buoy)
