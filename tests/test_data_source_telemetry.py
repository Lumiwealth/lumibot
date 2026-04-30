from __future__ import annotations


def test_data_source_telemetry_writes_csv_and_summary(monkeypatch, tmp_path):
    import lumibot.tools.data_source_telemetry as telemetry

    monkeypatch.setenv("LUMIBOT_DATA_SOURCE_TELEMETRY", "1")
    telemetry.reset_data_source_telemetry(for_testing=True)

    telemetry.record_data_source_event(
        category="remote_cache",
        action="s3_download",
        provider="ibkr",
        symbol="SPY",
        asset_type="stock",
        timestep="day",
        result="hit",
        elapsed_s=0.25,
        rows=10,
        bytes=1234,
        local_path=str(tmp_path / "spy.parquet"),
        remote_key="prod/cache/v1/ibkr/stock/day/spy.parquet",
    )

    summary = telemetry.data_source_telemetry_snapshot()
    assert summary["events_recorded"] == 1
    assert summary["elapsed_s_by_action"]["remote_cache.s3_download"]["count"] == 1
    assert summary["elapsed_s_by_action"]["remote_cache.s3_download"]["elapsed_s"] == 0.25

    settings_file = tmp_path / "run_settings.json"
    artifact = telemetry.write_data_source_telemetry_artifacts(str(settings_file))
    assert artifact["rows"] == 1
    assert (tmp_path / "run_data_source_telemetry.csv").exists()
    assert (tmp_path / "run_data_source_telemetry.parquet").exists()

    csv_text = (tmp_path / "run_data_source_telemetry.csv").read_text(encoding="utf-8")
    assert "remote_cache" in csv_text
    assert "s3_download" in csv_text


def test_data_source_telemetry_is_enabled_by_cache_miss_debug(monkeypatch):
    import lumibot.tools.data_source_telemetry as telemetry

    monkeypatch.delenv("LUMIBOT_DATA_SOURCE_TELEMETRY", raising=False)
    monkeypatch.setenv("LUMIBOT_CACHE_MISS_DEBUG", "1")
    telemetry.reset_data_source_telemetry(for_testing=True)

    telemetry.record_data_source_event(
        category="ibkr_history",
        action="cache_decision",
        provider="ibkr",
        result="miss",
        miss_reasons=["empty_cache"],
    )

    summary = telemetry.data_source_telemetry_snapshot()
    assert summary["enabled"] is True
    assert summary["events_recorded"] == 1


def test_data_source_telemetry_extra_json_is_valid(monkeypatch, tmp_path):
    import lumibot.tools.data_source_telemetry as telemetry

    monkeypatch.setenv("LUMIBOT_DATA_SOURCE_TELEMETRY", "1")
    telemetry.reset_data_source_telemetry(for_testing=True)

    telemetry.record_data_source_event(
        category="downloader_queue",
        action="queue_request",
        provider="ibkr",
        result="success",
        params={"conid": "123", "apiKey": "redacted upstream"},
    )

    settings = telemetry.data_source_telemetry_snapshot()
    assert settings["events_recorded"] == 1
    artifact = telemetry.write_data_source_telemetry_artifacts(str(tmp_path / "telemetry_settings.json"))
    assert artifact["rows"] == 1
