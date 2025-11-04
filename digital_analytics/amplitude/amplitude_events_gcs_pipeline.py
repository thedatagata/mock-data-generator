import dlt
from dlt.sources.filesystem import filesystem as src_fs
from dlt.destinations import filesystem as dest_fs
import polars as pl
from rich.console import Console
from rich.logging import RichHandler
import logging

console = Console()
logging.basicConfig(level=logging.INFO, handlers=[RichHandler(console=console)])

@dlt.resource(
    write_disposition="append",
    primary_key=['event_id', 'device_id']
)
def amplitude_events(file_path):
    """Process amplitude event files."""
    console.log(f"[blue]Reading: {file_path}")
    df = pl.read_parquet(file_path)
    console.log(f"[green]Loaded {len(df):,} rows")
    yield df.to_dicts()

if __name__ == '__main__':
    console.log("[bold cyan]Amplitude → GCS pipeline")
    
    pipeline = dlt.pipeline(
        pipeline_name="amplitude_to_gcs",
        destination=dest_fs(),  # Reads from config
        dataset_name="amplitude_events"
    )
    
    file_count = 0
    for file_object in src_fs():  # Reads from config
        file_path = file_object['file_url']
        file_count += 1
        console.log(f"[yellow]File {file_count}: {file_path}")
        
        info = pipeline.run(
            amplitude_events(file_path),
            loader_file_format="parquet"
        )
        console.log(f"[magenta]Load: {info.loads_ids}")
    
    console.log(f"[bold green]✓ {file_count} files")