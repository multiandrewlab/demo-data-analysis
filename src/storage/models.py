"""SQLAlchemy models for the profiler's SQLite output database."""

from sqlalchemy import (
    Boolean, Column, Float, Integer, String, Text, ForeignKey,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Table(Base):
    __tablename__ = "tables"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(String, nullable=False)  # 'mysql' or 'bigquery'
    database_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False)
    row_count = Column(Integer)
    discovery_status = Column(String, default="pending")
    profile_status = Column(String, default="pending")
    ratio_status = Column(String, default="pending")

    columns = relationship("ColumnModel", back_populates="table", cascade="all, delete-orphan")
    ratios = relationship("Ratio", back_populates="table", cascade="all, delete-orphan")


class ColumnModel(Base):
    __tablename__ = "columns"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_id = Column(Integer, ForeignKey("tables.id"), nullable=False)
    column_name = Column(String, nullable=False)
    data_type = Column(String, nullable=False)
    classification = Column(String)  # dimension/metric/identifier/temporal/other
    nullable = Column(Boolean)
    is_primary_key = Column(Boolean, default=False)
    is_foreign_key = Column(Boolean, default=False)
    fk_target_table = Column(String)
    fk_target_column = Column(String)

    table = relationship("Table", back_populates="columns")
    profile = relationship("ColumnProfile", back_populates="column", uselist=False,
                           cascade="all, delete-orphan")


class ColumnProfile(Base):
    __tablename__ = "column_profiles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    column_id = Column(Integer, ForeignKey("columns.id"), nullable=False, unique=True)
    null_rate = Column(Float)
    distinct_count = Column(Integer)
    min_value = Column(String)
    max_value = Column(String)
    mean = Column(Float)
    median = Column(Float)
    stddev = Column(Float)
    p5 = Column(Float)
    p25 = Column(Float)
    p75 = Column(Float)
    p95 = Column(Float)
    zero_rate = Column(Float)
    histogram_json = Column(Text)
    top_values_json = Column(Text)

    column = relationship("ColumnModel", back_populates="profile")


class Ratio(Base):
    __tablename__ = "ratios"

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_id = Column(Integer, ForeignKey("tables.id"), nullable=False)
    numerator_column_id = Column(Integer, ForeignKey("columns.id"), nullable=False)
    denominator_column_id = Column(Integer, ForeignKey("columns.id"), nullable=False)
    global_ratio = Column(Float)
    ratio_stddev = Column(Float)
    ratio_histogram_json = Column(Text)

    table = relationship("Table", back_populates="ratios")
    numerator_column = relationship("ColumnModel", foreign_keys=[numerator_column_id])
    denominator_column = relationship("ColumnModel", foreign_keys=[denominator_column_id])
    breakdowns = relationship("DimensionalBreakdown", back_populates="ratio",
                              cascade="all, delete-orphan")
    trends = relationship("TemporalTrend", back_populates="ratio",
                          cascade="all, delete-orphan")


class DimensionalBreakdown(Base):
    __tablename__ = "dimensional_breakdowns"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ratio_id = Column(Integer, ForeignKey("ratios.id"), nullable=False)
    dimension_column_id = Column(Integer, ForeignKey("columns.id"), nullable=False)
    dimension_value = Column(String)
    ratio_value = Column(Float)
    sample_size = Column(Integer)

    ratio = relationship("Ratio", back_populates="breakdowns")
    dimension_column = relationship("ColumnModel", foreign_keys=[dimension_column_id])


class CrossTableRelationship(Base):
    __tablename__ = "cross_table_relationships"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_table_id = Column(Integer, ForeignKey("tables.id"), nullable=False)
    target_table_id = Column(Integer, ForeignKey("tables.id"), nullable=False)
    relationship_type = Column(String)  # 'fk' or 'shared_dimension'
    join_column = Column(String)
    avg_cardinality = Column(Float)
    median_cardinality = Column(Float)

    source_table = relationship("Table", foreign_keys=[source_table_id])
    target_table = relationship("Table", foreign_keys=[target_table_id])


class TemporalTrend(Base):
    __tablename__ = "temporal_trends"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ratio_id = Column(Integer, ForeignKey("ratios.id"), nullable=False)
    time_bucket = Column(String)
    ratio_value = Column(Float)
    sample_size = Column(Integer)

    ratio = relationship("Ratio", back_populates="trends")


def create_database(db_path: str):
    """Create a SQLite database with all tables. Returns the engine."""
    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    return engine
