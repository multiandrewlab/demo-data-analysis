"""CLI entry point for the synthetic data profiler."""

import logging
import sys

import click
from rich.console import Console
from rich.table import Table as RichTable

from src.config import load_config
from src.storage.models import create_database
from src.storage.store import ProfileStore

console = Console()
logger = logging.getLogger("profiler")


def _setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )


@click.group()
def cli():
    """Synthetic Data Profiler — analyze tables for realistic data generation."""
    pass


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
def discover(config_path: str):
    """Phase 1: Discover schema from MySQL and BigQuery."""
    config = load_config(config_path)
    _setup_logging(config.output.log_level)
    store = ProfileStore(config.output.database_path)

    try:
        from src.discovery.schema import SchemaDiscoverer

        mysql_conn = None
        bq_conns = {}

        if config.mysql.databases:
            from src.connections.mysql import MySQLConnection
            mysql_conn = MySQLConnection(config.mysql)

        if config.bigquery.projects:
            from src.connections.bigquery import BigQueryConnection
            for proj_cfg in config.bigquery.projects:
                if proj_cfg.project:
                    bq_conns[proj_cfg.project] = BigQueryConnection(
                        config.bigquery, project=proj_cfg.project
                    )

        discoverer = SchemaDiscoverer(config, store,
                                      mysql_conn=mysql_conn, bq_conns=bq_conns)
        discoverer.discover_all()
        console.print("[green]Discovery complete.[/green]")
    finally:
        store.close()


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
def profile(config_path: str):
    """Phase 2: Profile columns with adaptive sampling."""
    config = load_config(config_path)
    _setup_logging(config.output.log_level)
    store = ProfileStore(config.output.database_path)

    try:
        from src.profiling.profiler import ColumnProfiler
        from src.profiling.sampler import AdaptiveSampler
        from src.connections.mysql import MySQLConnection
        from src.connections.bigquery import BigQueryConnection

        profiler = ColumnProfiler()
        sampler = AdaptiveSampler(config.sampling)

        tables = store.get_tables_by_status("profile_status", "pending")
        console.print(f"[cyan]Profiling {len(tables)} tables...[/cyan]")

        import pandas as pd

        for tbl in tables:
            store.update_table_status(tbl.id, "profile_status", "in_progress")
            columns = store.get_columns_for_table(tbl.id)

            # Get the right connection
            if tbl.source == "mysql":
                conn = MySQLConnection(config.mysql)
                pk_cols = [c for c in columns if c.is_primary_key]
                pk_col = pk_cols[0].column_name if pk_cols else None

                query = sampler.build_sample_query(
                    "mysql", tbl.database_name, tbl.table_name,
                    row_count=tbl.row_count, pk_column=pk_col,
                )
                rows = conn.execute_query(query, tbl.database_name)
                df = pd.DataFrame(rows)
                conn.dispose()
            elif tbl.source == "bigquery":
                # Find the right project connection for this dataset
                bq = None
                for proj_cfg in config.bigquery.projects:
                    if tbl.database_name in proj_cfg.datasets:
                        bq = BigQueryConnection(config.bigquery, project=proj_cfg.project)
                        break
                if bq is None:
                    logger.warning("No BQ project found for dataset %s", tbl.database_name)
                    store.update_table_status(tbl.id, "profile_status", "skipped")
                    continue

                query = sampler.build_sample_query(
                    "bigquery", tbl.database_name, tbl.table_name,
                    row_count=tbl.row_count,
                )
                try:
                    result = bq.execute_query(query)
                    df = result.to_dataframe()
                except Exception as e:
                    logger.warning("Skipping %s.%s: %s", tbl.database_name,
                                  tbl.table_name, e)
                    store.update_table_status(tbl.id, "profile_status", "skipped")
                    continue
                finally:
                    bq.dispose()
            else:
                continue

            if df.empty:
                store.update_table_status(tbl.id, "profile_status", "completed")
                continue

            for col in columns:
                if col.classification == "other":
                    continue
                if col.column_name not in df.columns:
                    continue

                result = profiler.profile_column(df[col.column_name], col.classification)
                store.save_column_profile(col.id, **result)

            store.update_table_status(tbl.id, "profile_status", "completed")
            console.print(f"  [green]Profiled {tbl.database_name}.{tbl.table_name}[/green]")

        console.print("[green]Profiling complete.[/green]")
    finally:
        store.close()


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
def ratios(config_path: str):
    """Phase 3: Detect ratios and compute dimensional breakdowns."""
    config = load_config(config_path)
    _setup_logging(config.output.log_level)
    store = ProfileStore(config.output.database_path)

    try:
        from src.ratios.detector import RatioDetector
        from src.ratios.calculator import RatioCalculator
        from src.profiling.sampler import AdaptiveSampler
        from src.connections.mysql import MySQLConnection
        from src.connections.bigquery import BigQueryConnection

        detector = RatioDetector()
        calculator = RatioCalculator()
        sampler = AdaptiveSampler(config.sampling)

        tables = store.get_tables_by_status("ratio_status", "pending")
        # Only process tables that have been profiled
        tables = [t for t in tables if t.profile_status == "completed"]

        console.print(f"[cyan]Analyzing ratios for {len(tables)} tables...[/cyan]")

        import pandas as pd

        for tbl in tables:
            store.update_table_status(tbl.id, "ratio_status", "in_progress")
            columns = store.get_columns_for_table(tbl.id)

            metric_cols = [c for c in columns if c.classification == "metric"]
            dimension_cols = [c for c in columns if c.classification == "dimension"]
            temporal_cols = [c for c in columns if c.classification == "temporal"]

            if len(metric_cols) < 2:
                store.update_table_status(tbl.id, "ratio_status", "completed")
                continue

            # Fetch data sample
            if tbl.source == "mysql":
                conn = MySQLConnection(config.mysql)
                pk_cols = [c for c in columns if c.is_primary_key]
                pk_col = pk_cols[0].column_name if pk_cols else None
                query = sampler.build_sample_query(
                    "mysql", tbl.database_name, tbl.table_name,
                    row_count=tbl.row_count, pk_column=pk_col,
                )
                rows = conn.execute_query(query, tbl.database_name)
                df = pd.DataFrame(rows)
                conn.dispose()
            elif tbl.source == "bigquery":
                bq = None
                for proj_cfg in config.bigquery.projects:
                    if tbl.database_name in proj_cfg.datasets:
                        bq = BigQueryConnection(config.bigquery, project=proj_cfg.project)
                        break
                if bq is None:
                    store.update_table_status(tbl.id, "ratio_status", "skipped")
                    continue

                query = sampler.build_sample_query(
                    "bigquery", tbl.database_name, tbl.table_name,
                    row_count=tbl.row_count,
                )
                try:
                    result = bq.execute_query(query)
                    df = result.to_dataframe()
                except Exception as e:
                    logger.warning("Skipping ratios for %s.%s: %s",
                                  tbl.database_name, tbl.table_name, e)
                    store.update_table_status(tbl.id, "ratio_status", "skipped")
                    continue
                finally:
                    bq.dispose()
            else:
                continue

            if df.empty:
                store.update_table_status(tbl.id, "ratio_status", "completed")
                continue

            # Detect plausible pairs
            metric_names = [c.column_name for c in metric_cols
                           if c.column_name in df.columns]
            pairs = detector.find_plausible_pairs(df, metric_names)

            col_name_to_id = {c.column_name: c.id for c in columns}

            for num_col, den_col in pairs:
                # Global ratio
                ratio_result = calculator.compute_ratio(df, num_col, den_col)
                ratio_id = store.save_ratio(
                    tbl.id, col_name_to_id[num_col], col_name_to_id[den_col],
                    global_ratio=ratio_result["global_ratio"],
                    ratio_stddev=ratio_result["ratio_stddev"],
                    ratio_histogram_json=ratio_result["ratio_histogram_json"],
                )

                # Dimensional breakdowns
                for dim_col in dimension_cols:
                    if dim_col.column_name not in df.columns:
                        continue
                    breakdowns = calculator.compute_dimensional_breakdown(
                        df, num_col, den_col, dim_col.column_name
                    )
                    for bd in breakdowns:
                        store.save_dimensional_breakdown(
                            ratio_id, col_name_to_id[dim_col.column_name],
                            bd["dimension_value"], bd["ratio_value"],
                            sample_size=bd["sample_size"],
                        )

                # Temporal trends
                for temp_col in temporal_cols:
                    if temp_col.column_name not in df.columns:
                        continue
                    trends = calculator.compute_temporal_trend(
                        df, num_col, den_col, temp_col.column_name
                    )
                    for trend in trends:
                        store.save_temporal_trend(
                            ratio_id, trend["time_bucket"],
                            trend["ratio_value"], sample_size=trend["sample_size"],
                        )

            store.update_table_status(tbl.id, "ratio_status", "completed")
            console.print(f"  [green]Ratios for {tbl.database_name}.{tbl.table_name}: "
                         f"{len(pairs)} pairs found[/green]")

        console.print("[green]Ratio analysis complete.[/green]")
    finally:
        store.close()


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
def analyze(config_path: str):
    """Run full pipeline: discover -> profile -> ratios."""
    ctx = click.get_current_context()
    ctx.invoke(discover, config_path=config_path)
    ctx.invoke(profile, config_path=config_path)
    ctx.invoke(ratios, config_path=config_path)


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
def status(config_path: str):
    """Show current progress across all phases."""
    config = load_config(config_path)
    store = ProfileStore(config.output.database_path)

    try:
        tables = store.get_all_tables()

        if not tables:
            console.print("[yellow]No tables discovered yet.[/yellow]")
            return

        # Summary counts
        summary = RichTable(title="Pipeline Status")
        summary.add_column("Phase")
        summary.add_column("Pending", justify="right")
        summary.add_column("In Progress", justify="right")
        summary.add_column("Completed", justify="right")
        summary.add_column("Skipped", justify="right")

        for phase in ["discovery_status", "profile_status", "ratio_status"]:
            counts = {}
            for t in tables:
                s = getattr(t, phase)
                counts[s] = counts.get(s, 0) + 1
            summary.add_row(
                phase.replace("_status", "").title(),
                str(counts.get("pending", 0)),
                str(counts.get("in_progress", 0)),
                str(counts.get("completed", 0)),
                str(counts.get("skipped", 0)),
            )

        console.print(summary)
        console.print(f"\n[cyan]Total tables: {len(tables)}[/cyan]")
    finally:
        store.close()


@cli.command()
@click.option("--config", "config_path", required=True, help="Path to settings.yaml")
@click.option("--phase", type=click.Choice(["discover", "profile", "ratios"]),
              required=True, help="Phase to reset")
def reset(config_path: str, phase: str):
    """Reset a phase to re-run it."""
    config = load_config(config_path)
    store = ProfileStore(config.output.database_path)

    status_field = {
        "discover": "discovery_status",
        "profile": "profile_status",
        "ratios": "ratio_status",
    }[phase]

    try:
        tables = store.get_all_tables()
        for tbl in tables:
            store.update_table_status(tbl.id, status_field, "pending")
        console.print(f"[green]Reset {len(tables)} tables for {phase} phase.[/green]")
    finally:
        store.close()


if __name__ == "__main__":
    cli()
