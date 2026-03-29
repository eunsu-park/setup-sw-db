"""Database core functions."""
import pandas as pd
from egghouse.database import PostgresManager


def initialize_database(db_config: dict, schema_config: dict,
                        verbose: bool = True) -> None:
    """Initialize database and create tables.

    Convenience function that combines create_database and create_tables.

    Args:
        db_config: Database configuration dict.
        schema_config: Schema configuration dict with table definitions.
        verbose: If True, print status messages.
    """
    if verbose:
        print("[Database Initialization]")
    create_database(db_config)
    create_tables(db_config, schema_config)
    if verbose:
        print()


def create_database(db_config: dict):
    """Create database if it does not exist."""
    db_name = db_config['database']

    # Check if database already exists
    try:
        with PostgresManager(**db_config) as db:
            print(f"✓ {db_name} already exists")
            return True
    except Exception:
        pass

    # Try creating via admin databases
    for admin_db in ['template1', 'postgres']:
        try:
            admin_config = {**db_config, 'database': admin_db}
            with PostgresManager(**admin_config) as db:
                exists = db.execute(
                    f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'",
                    fetch=True
                )
                if not exists:
                    db.execute(f"CREATE DATABASE {db_name}")
                    print(f"✓ {db_name} created")
                else:
                    print(f"✓ {db_name} already exists")
                return True
        except Exception:
            continue

    print(f"✗ {db_name} creation failed - manual creation required")
    return False


def create_tables(db_config: dict, schema_config: dict, drop: bool = False):
    """Create all tables from schema_config.

    Supports extended schema format with:
    - _primary_key: list of columns for composite primary key
    - _unique: list of columns for UNIQUE constraint
    - _indexes: list of column lists for indexes

    Args:
        db_config: Database configuration dict.
        schema_config: Schema configuration dict with table definitions.
        drop: If True, drop existing tables before creating.
    """
    with PostgresManager(**db_config) as db:
        existing = [t['name'] for t in db.list_tables()]

        for name, schema in schema_config.items():
            if name in existing:
                if drop:
                    db.execute(f"DROP TABLE IF EXISTS {name} CASCADE")
                    print(f"  {name}: 삭제 후 재생성")
                else:
                    print(f"  {name}: 이미 존재 (skip)")
                    continue

            # Extract metadata from schema
            primary_key = schema.pop('_primary_key', None)
            unique_columns = schema.pop('_unique', None)
            indexes = schema.pop('_indexes', None)

            # Check if schema uses new format (with _primary_key) or old format
            if primary_key:
                # New format: build CREATE TABLE manually
                _create_table_with_composite_pk(
                    db, name, schema, primary_key, unique_columns, indexes
                )
            else:
                # Old format: use egghouse's create_table
                db.create_table(name, schema)

            # Restore metadata for potential reuse
            if primary_key:
                schema['_primary_key'] = primary_key
            if unique_columns:
                schema['_unique'] = unique_columns
            if indexes:
                schema['_indexes'] = indexes

            col_count = len([k for k in schema.keys() if not k.startswith('_')])
            print(f"  {name}: 생성 ({col_count} 컬럼)")


def _create_table_with_composite_pk(db, table_name: str, schema: dict,
                                     primary_key: list, unique_columns: list = None,
                                     indexes: list = None):
    """Create table with composite primary key support.

    Args:
        db: PostgresManager instance.
        table_name: Name of the table to create.
        schema: Column definitions (column_name: TYPE).
        primary_key: List of columns for composite primary key.
        unique_columns: List of columns for UNIQUE constraint.
        indexes: List of column lists for indexes.
    """
    # Build column definitions
    columns = []
    for col_name, col_type in schema.items():
        if col_name.startswith('_'):
            continue
        # Remove PRIMARY KEY from individual columns (will use composite)
        col_type_clean = col_type.replace(' PRIMARY KEY', '')
        columns.append(f"{col_name} {col_type_clean}")

    # Add composite primary key
    pk_cols = ', '.join(primary_key)
    columns.append(f"PRIMARY KEY ({pk_cols})")

    # Add UNIQUE constraint
    if unique_columns:
        if isinstance(unique_columns, str):
            unique_columns = [unique_columns]
        for col in unique_columns:
            columns.append(f"UNIQUE ({col})")

    # Create table
    sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
    db.execute(sql)

    # Create indexes
    if indexes:
        for idx_cols in indexes:
            if isinstance(idx_cols, str):
                idx_cols = [idx_cols]
            idx_name = f"idx_{table_name}_{'_'.join(idx_cols)}"
            idx_cols_str = ', '.join(idx_cols)
            db.execute(f"CREATE INDEX {idx_name} ON {table_name} ({idx_cols_str})")


def insert(df: pd.DataFrame, table: str, db_config: dict, replace_key: dict = None, batch: int = 1000) -> int:
    """Insert DataFrame into database.

    Args:
        df: DataFrame to insert.
        table: Table name.
        db_config: Database configuration.
        replace_key: Delete condition before insert (e.g. {'year': 2023}).
            Uses transaction to prevent data loss on insert failure.
        batch: Batch size for insert.

    Returns:
        Number of records inserted.
    """
    df.columns = df.columns.str.lower()
    records = df.to_dict('records')

    # NaN → None
    for rec in records:
        for k, v in rec.items():
            if pd.isna(v):
                rec[k] = None

    if not records:
        return 0

    with PostgresManager(**db_config) as db:
        if replace_key:
            # Transaction: rollback delete if insert fails
            db.execute("BEGIN")
            try:
                db.delete(table, where=replace_key)
                inserted = 0
                for i in range(0, len(records), batch):
                    db.insert(table, records[i:i + batch])
                    inserted += len(records[i:i + batch])
                db.execute("COMMIT")
                return inserted
            except Exception as e:
                db.execute("ROLLBACK")
                print(f"  Insert failed (rolled back): {e}")
                return 0

        inserted = 0
        failed = 0
        for i in range(0, len(records), batch):
            try:
                db.insert(table, records[i:i + batch])
                inserted += len(records[i:i + batch])
            except Exception as e:
                failed += len(records[i:i + batch])
                print(f"  Batch error: {e}")

        if failed:
            print(f"  Warning: {failed}/{len(records)} records failed to insert")
        return inserted


def upsert(df: pd.DataFrame, table: str, db_config: dict,
           conflict_columns: str | list[str] = 'datetime', batch: int = 1000) -> int:
    """Insert records with ON CONFLICT DO NOTHING.

    Inserts new records and silently skips duplicates based on conflict columns.
    Supports composite primary keys by accepting multiple conflict columns.

    Args:
        df: DataFrame to insert.
        table: Table name.
        db_config: Database configuration.
        conflict_columns: Column(s) to check for conflicts.
            Can be a string for single column or list for composite PK.
            Default: 'datetime'.
        batch: Batch size for insert.

    Returns:
        Number of records inserted (excludes skipped duplicates).
    """
    df.columns = df.columns.str.lower()
    records = df.to_dict('records')

    # NaN → None
    for rec in records:
        for k, v in rec.items():
            if pd.isna(v):
                rec[k] = None

    if not records:
        return 0

    # Normalize conflict_columns to list
    if isinstance(conflict_columns, str):
        conflict_columns = [conflict_columns]

    # Build conflict columns string for composite PK
    conflict_cols_str = ', '.join(conflict_columns)

    try:
        with PostgresManager(**db_config) as db:
            inserted = 0
            skipped = 0
            columns = list(records[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)

            # SQL with composite conflict columns
            sql = f"""
                INSERT INTO {table} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_cols_str}) DO NOTHING
            """

            for i in range(0, len(records), batch):
                batch_records = records[i:i + batch]
                for rec in batch_records:
                    values = tuple(rec[col] for col in columns)
                    try:
                        db.execute(sql, values)
                        inserted += 1
                    except Exception as e:
                        # Handle other unique constraint violations (e.g., file_path)
                        if 'duplicate key' in str(e).lower() or 'unique' in str(e).lower():
                            skipped += 1
                        else:
                            raise

            return inserted
    except Exception as e:
        print(f"  Upsert error: {e}")
        import traceback
        traceback.print_exc()
        return 0


def delete_orphans(table: str, db_config: dict, file_column: str = 'file_path') -> int:
    """Delete records where the referenced file no longer exists.

    Args:
        table: Table name.
        db_config: Database configuration.
        file_column: Column containing file paths.

    Returns:
        Number of orphan records deleted.
    """
    from pathlib import Path

    with PostgresManager(**db_config) as db:
        # Get all file paths from DB
        result = db.execute(f"SELECT {file_column} FROM {table}", fetch=True)
        if not result:
            return 0

        orphan_paths = []
        for row in result:
            file_path = row[file_column]
            if not Path(file_path).exists():
                orphan_paths.append(file_path)

        if not orphan_paths:
            return 0

        # Delete orphans in batches
        deleted = 0
        for path in orphan_paths:
            try:
                db.execute(
                    f"DELETE FROM {table} WHERE {file_column} = %s",
                    (path,)
                )
                deleted += 1
            except Exception as e:
                print(f"  Delete error: {e}")

        return deleted