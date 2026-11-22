"""S7, my own tests: the two tools, with serving and the warehouse faked."""

import json

import pytest
from mcp.server.fastmcp.exceptions import ToolError

import labmcp.server as s


async def call(name, args):
    return json.loads((await s.mcp.call_tool(name, args))[0].text)


async def test_predict_duration_sends_ids_hour_and_a_sunday_zero_dow(monkeypatch):
    seen = []

    async def champion(method, path, body=None):
        seen.append((method, path, body))
        return {"duration_minutes": 31.4, "model": {"name": "trip-duration", "version": "1", "alias": "champion"}}

    monkeypatch.setattr(s, "champion", champion)
    out = await call("predict_duration", {"pickup_zone": "JFK Airport", "dropoff_zone": "Times Sq/Theatre District", "hour": 17, "day_of_week": "friday"})
    assert seen == [("POST", "/predict", {"pickup_zone_id": 132, "dropoff_zone_id": 230, "pickup_hour": 17, "pickup_dow": 5})]
    assert out["duration_minutes"] == 31.4 and out["model"]["alias"] == "champion"


async def test_predict_duration_refuses_an_unknown_zone_with_a_suggestion(monkeypatch):
    async def champion(*a, **k):
        raise AssertionError("serving must not be called for an unknown zone")

    monkeypatch.setattr(s, "champion", champion)
    with pytest.raises(ToolError, match="did you mean 'jfk airport'"):
        await s.mcp.call_tool("predict_duration", {"pickup_zone": "JFK Airprot", "dropoff_zone": "JFK Airport", "hour": 1, "day_of_week": "monday"})


class FakeWarehouse:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    def query(self, sql, params=None, timeout_s=30):
        self.calls.append((sql, params))
        return self.rows


def row(hour, **kw):
    return {"zone": "JFK Airport", "pickup_hour": hour, "trip_count": 100 + hour, "median_duration_min": 20.0 + hour, "p90_duration_min": 40.0 + hour,
            "first_month": "2024-01", "last_month": "2024-04"} | kw


async def test_zone_stats_uses_constant_sql_and_named_parameters(monkeypatch):
    fake = FakeWarehouse([row(17)])
    monkeypatch.setattr(s, "warehouse", lambda: fake)
    out = await call("query_zone_stats", {"zone": "JFK Airport", "metric": "median_duration_min", "hour": 17})
    assert out == {"zone": "JFK Airport", "metric": "median_duration_min", "covers": "2024-01..2024-04", "hour": 17, "value": 37.0}
    assert fake.calls == [(s.ZONE_STATS_HOUR_SQL, {"zone_id": 132, "hour": 17})]


async def test_zone_stats_without_an_hour_returns_every_hour(monkeypatch):
    monkeypatch.setattr(s, "warehouse", lambda: FakeWarehouse([row(h) for h in (8, 9)]))
    out = await call("query_zone_stats", {"zone": "JFK Airport", "metric": "trip_count"})
    assert out["by_hour"] == {"8": 108, "9": 109}


async def test_an_injection_in_the_zone_never_reaches_sql(monkeypatch):
    fake = FakeWarehouse([])
    monkeypatch.setattr(s, "warehouse", lambda: fake)
    with pytest.raises(ToolError, match="unknown TLC zone"):
        await s.mcp.call_tool("query_zone_stats", {"zone": "JFK Airport'; drop table marts.zone_hour_stats; --", "metric": "trip_count"})
    assert fake.calls == []


async def test_a_metric_outside_the_list_is_refused(monkeypatch):
    monkeypatch.setattr(s, "warehouse", lambda: FakeWarehouse([]))
    with pytest.raises(ToolError):
        await s.mcp.call_tool("query_zone_stats", {"zone": "JFK Airport", "metric": "trip_count; select 1"})


async def test_a_zone_with_no_rows_says_so(monkeypatch):
    monkeypatch.setattr(s, "warehouse", lambda: FakeWarehouse([]))
    with pytest.raises(ToolError, match="no statistics"):
        await s.mcp.call_tool("query_zone_stats", {"zone": "JFK Airport", "metric": "trip_count", "hour": 3})
