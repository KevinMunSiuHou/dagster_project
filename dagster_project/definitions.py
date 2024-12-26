from . import assets
from .jobs import SCHEDULES
from dagster import Definitions, load_assets_from_modules

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    schedules = SCHEDULES
)
