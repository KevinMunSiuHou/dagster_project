from dagster import AssetKey, load_assets_from_package_module
from . import temporary_usage

temporary_usage_assets = load_assets_from_package_module(
    package_module = temporary_usage, key_prefix = ['temporary_usage']
)