"""CRUD operations for the profiler's SQLite database."""

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from src.storage.models import (
    Base, Table, ColumnModel, ColumnProfile, Ratio,
    DimensionalBreakdown, CrossTableRelationship, TemporalTrend,
)


_ALLOWED_STATUS_FIELDS = {"discovery_status", "profile_status", "ratio_status"}


class ProfileStore:
    def __init__(self, db_path: str):
        self.engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(self.engine)
        self._session = Session(self.engine)

    def close(self):
        self._session.close()
        self.engine.dispose()

    # --- Tables ---

    def upsert_table(self, source: str, database_name: str, table_name: str,
                     row_count: int | None = None) -> int:
        existing = self._session.execute(
            select(Table).where(
                Table.source == source,
                Table.database_name == database_name,
                Table.table_name == table_name,
            )
        ).scalar_one_or_none()

        if existing:
            if row_count is not None:
                existing.row_count = row_count
            existing.discovery_status = "completed"
            self._session.commit()
            return existing.id

        tbl = Table(source=source, database_name=database_name, table_name=table_name,
                    row_count=row_count, discovery_status="completed")
        self._session.add(tbl)
        self._session.commit()
        return tbl.id

    def get_table(self, table_id: int) -> Table:
        return self._session.get(Table, table_id)

    def get_all_tables(self) -> list[Table]:
        return list(self._session.execute(select(Table)).scalars().all())

    def update_table_status(self, table_id: int, status_field: str, status_value: str):
        if status_field not in _ALLOWED_STATUS_FIELDS:
            raise ValueError(f"Invalid status field: {status_field!r}")
        tbl = self._session.get(Table, table_id)
        setattr(tbl, status_field, status_value)
        self._session.commit()

    def get_tables_by_status(self, status_field: str, status_value: str) -> list[Table]:
        if status_field not in _ALLOWED_STATUS_FIELDS:
            raise ValueError(f"Invalid status field: {status_field!r}")
        return list(self._session.execute(
            select(Table).where(getattr(Table, status_field) == status_value)
        ).scalars().all())

    # --- Columns ---

    def add_column(self, table_id: int, column_name: str, data_type: str,
                   classification: str | None = None, nullable: bool = True,
                   is_primary_key: bool = False, is_foreign_key: bool = False,
                   fk_target_table: str | None = None,
                   fk_target_column: str | None = None) -> int:
        existing = self._session.execute(
            select(ColumnModel).where(
                ColumnModel.table_id == table_id,
                ColumnModel.column_name == column_name,
            )
        ).scalar_one_or_none()

        if existing:
            existing.data_type = data_type
            existing.classification = classification
            existing.nullable = nullable
            existing.is_primary_key = is_primary_key
            existing.is_foreign_key = is_foreign_key
            existing.fk_target_table = fk_target_table
            existing.fk_target_column = fk_target_column
            self._session.commit()
            return existing.id

        col = ColumnModel(
            table_id=table_id, column_name=column_name, data_type=data_type,
            classification=classification, nullable=nullable,
            is_primary_key=is_primary_key, is_foreign_key=is_foreign_key,
            fk_target_table=fk_target_table, fk_target_column=fk_target_column,
        )
        self._session.add(col)
        self._session.commit()
        return col.id

    def get_column(self, column_id: int) -> ColumnModel:
        return self._session.get(ColumnModel, column_id)

    def get_columns_for_table(self, table_id: int,
                              classification: str | None = None) -> list[ColumnModel]:
        stmt = select(ColumnModel).where(ColumnModel.table_id == table_id)
        if classification:
            stmt = stmt.where(ColumnModel.classification == classification)
        return list(self._session.execute(stmt).scalars().all())

    # --- Column Profiles ---

    def save_column_profile(self, column_id: int, **kwargs):
        existing = self._session.execute(
            select(ColumnProfile).where(ColumnProfile.column_id == column_id)
        ).scalar_one_or_none()

        if existing:
            for k, v in kwargs.items():
                setattr(existing, k, v)
        else:
            profile = ColumnProfile(column_id=column_id, **kwargs)
            self._session.add(profile)
        self._session.commit()

    def get_column_profile(self, column_id: int) -> ColumnProfile | None:
        return self._session.execute(
            select(ColumnProfile).where(ColumnProfile.column_id == column_id)
        ).scalar_one_or_none()

    # --- Ratios ---

    def save_ratio(self, table_id: int, numerator_column_id: int,
                   denominator_column_id: int, global_ratio: float,
                   ratio_stddev: float | None = None,
                   ratio_histogram_json: str | None = None) -> int:
        ratio = Ratio(
            table_id=table_id, numerator_column_id=numerator_column_id,
            denominator_column_id=denominator_column_id,
            global_ratio=global_ratio, ratio_stddev=ratio_stddev,
            ratio_histogram_json=ratio_histogram_json,
        )
        self._session.add(ratio)
        self._session.commit()
        return ratio.id

    def get_ratios_for_table(self, table_id: int) -> list[Ratio]:
        return list(self._session.execute(
            select(Ratio).where(Ratio.table_id == table_id)
        ).scalars().all())

    def delete_ratios_for_table(self, table_id: int):
        """Delete all ratios (and cascaded breakdowns/trends) for a table."""
        ratios = self.get_ratios_for_table(table_id)
        for r in ratios:
            self._session.delete(r)
        self._session.commit()

    # --- Dimensional Breakdowns ---

    def save_dimensional_breakdown(self, ratio_id: int, dimension_column_id: int,
                                   dimension_value: str, ratio_value: float,
                                   sample_size: int | None = None):
        bd = DimensionalBreakdown(
            ratio_id=ratio_id, dimension_column_id=dimension_column_id,
            dimension_value=dimension_value, ratio_value=ratio_value,
            sample_size=sample_size,
        )
        self._session.add(bd)
        self._session.commit()

    def get_breakdowns_for_ratio(self, ratio_id: int) -> list[DimensionalBreakdown]:
        return list(self._session.execute(
            select(DimensionalBreakdown).where(DimensionalBreakdown.ratio_id == ratio_id)
        ).scalars().all())

    # --- Cross-Table Relationships ---

    def save_cross_table_relationship(self, source_table_id: int, target_table_id: int,
                                      relationship_type: str, join_column: str,
                                      avg_cardinality: float,
                                      median_cardinality: float | None = None):
        rel = CrossTableRelationship(
            source_table_id=source_table_id, target_table_id=target_table_id,
            relationship_type=relationship_type, join_column=join_column,
            avg_cardinality=avg_cardinality, median_cardinality=median_cardinality,
        )
        self._session.add(rel)
        self._session.commit()

    # --- Temporal Trends ---

    def save_temporal_trend(self, ratio_id: int, time_bucket: str,
                            ratio_value: float, sample_size: int | None = None):
        trend = TemporalTrend(
            ratio_id=ratio_id, time_bucket=time_bucket,
            ratio_value=ratio_value, sample_size=sample_size,
        )
        self._session.add(trend)
        self._session.commit()

    def get_trends_for_ratio(self, ratio_id: int) -> list[TemporalTrend]:
        return list(self._session.execute(
            select(TemporalTrend).where(TemporalTrend.ratio_id == ratio_id)
        ).scalars().all())
