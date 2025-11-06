from sling import Replication, ReplicationStream, Mode

SOURCE_CONFIGS = [
    {
        "source_name": "actionbuilder_pa_mpa",
        "source_schema": "mpa",
        "dest_schema": "raw_actionbuilder__pa_mpa_sling",
    },
    # {
    #     "source_name": "actionbuilder_pa_fwa",
    #     "source_schema": "firelands",
    #     "dest_schema": "raw_actionbuilder__pa_fwa_sling",
    # },
]

BASE_STREAM_CONFIGS = {
    "*": {"primary_key": "id", "update_key": "updated_at", "mode": Mode.INCREMENTAL},
    # Small tables that don't follow default
    "addresses_electoral_districts": {
        "primary_key": ["address_id", "electoral_district_ocd_id"],
        "update_key": None,
        "mode": Mode.TRUNCATE,
    },
    "electoral_districts": {
        "primary_key": "ocd_id",
        "update_key": None,
        "mode": Mode.TRUNCATE,
    },
    "active_storage_variant_records": {"update_key": None, "mode": Mode.TRUNCATE},
    "active_storage_blobs": {"update_key": None, "mode": Mode.TRUNCATE},
    "active_storage_attachments": {"update_key": None, "mode": Mode.TRUNCATE},
    # Metadata tables we don't need to load
    "versions": {"disabled": True},
    "ar_internal_metadata": {"disabled": True},
    "schema_migrations": {"disabled": True},
}

for source_config in SOURCE_CONFIGS:
    stream_configs = {
        f"{source_config['source_schema']}.{key}": {
            **value,
            "object": f"{source_config["dest_schema"]}" + "{stream_table}",
        }
        for key, value in BASE_STREAM_CONFIGS.items()
    }

    for chunked_table in [
        "taggable_logbook",
        "entity_sync_stored_operations",
        "action_entities",
    ]:
        stream_configs[chunked_table] = {
            "sql": f"select * from {source_config['source_schema']}.{chunked_table}"
            + " where {incremental_where_cond}",
            "update_key": "id",
            "source_options": {"chunk_size": 10000},
            "object": f"{source_config['dest_schema']}.{chunked_table}"
        }

    print(stream_configs)

    replication = Replication(
        source=source_config["source_name"],
        target="bigquery_data_warehouse",
        streams=stream_configs,
        # env=dict(
        #     SLING_THREADS=8,
        #     SLING_RETRIES=3,
        # ),
        debug=True,
    )

    replication.run()
