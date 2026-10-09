import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


wrong_tool_db_connection = SlingConnectionResource(
    name="WRONG_TOOL_DB",
    type="postgres",
    connection_string=EnvVar("WRONG_TOOL_DATABASE_URL"),
)


wrong_tool_replication_config = {
    "source": "WRONG_TOOL_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "wrong_tool.{stream_table}",
    },

    "streams": {
        "public.active_storage_attachments": None,
        "public.active_storage_blobs": None,
        "public.active_storage_variant_records": {
            "select": [
                "-variation_digest",
            ],
        },
        "public.buddy_pomodoros": None,
        "public.metric_snapshots": None,
        "public.nudges": {
            "select": [
                "-token",
            ],
        },
        "public.pairs": None,
        "public.projects": {
            "select": [
                "-buddy_code",
            ],
        },
        "public.rewards": None,
        "public.ships": None,
        "public.streak_activities": None,
        "public.users": {
            "select": [
                "-hackatime_access_token",
            ],
        },
    },
}


@dg.asset(
    name="wrong_tool_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def wrong_tool_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Wrong Tool DB → warehouse in a single shot."""
    context.log.info("Starting Wrong Tool → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=wrong_tool_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None
