
from delta.tables import DeltaTable


def merge_weather(spark, source_df, target_path):

    target_path = str(target_path)

    if not DeltaTable.isDeltaTable(
        spark, target_path
    ):
        (
            source_df.write
            .format("delta")
            .mode("errorifexists")
            .save(target_path)
        )

        print("Silver Delta table created")
        return

    target = DeltaTable.forPath(
        spark, target_path
    )

    condition = """
        target.batch_id = source.batch_id
        AND target.city = source.city
        AND target.forecast_valid_at_utc =
            source.forecast_valid_at_utc
    """

    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            condition
        )
        .whenMatchedUpdateAll(
            condition="""
                source.ingested_at_utc >
                target.ingested_at_utc
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Silver Delta MERGE completed")


def merge_quarantine(spark, rejected_df, target_path):

    target_path = str(target_path)

    rejected_df = rejected_df.dropDuplicates(
        ["rejection_id"]
    )

    if not DeltaTable.isDeltaTable(
        spark, target_path
    ):
        (
            rejected_df.write
            .format("delta")
            .mode("errorifexists")
            .save(target_path)
        )
        return

    target = DeltaTable.forPath(
        spark, target_path
    )

    (
        target.alias("target")
        .merge(
            rejected_df.alias("source"),
            "target.rejection_id = source.rejection_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Quarantine Delta MERGE completed")
