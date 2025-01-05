from dagster import AssetSelection, define_asset_job, ScheduleDefinition

job__trading_view_market_data = define_asset_job(
    "trading_view_market_data_job",
    selection = (
        AssetSelection.keys(
            "temporary_usage/extract_trading_view_xauusd",
            "temporary_usage/compare_trading_view_xauusd",
            "temporary_usage/extract_trading_view_btcusd",
            "temporary_usage/compare_trading_view_btcusd"
        )
    ),
)

schedule__trading_view_market_data = ScheduleDefinition(
    name = "job__trading_view_market_data",
    job = job__trading_view_market_data,
    cron_schedule = "0 * * * *", execution_timezone = "Asia/Kuala_Lumpur"
)