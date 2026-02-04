"""FlowGuard Main CLI

Command-line interface for managing FlowGuard services and utilities.
"""

import click
import logging
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    """FlowGuard - Real-time Event Processing Pipeline
    
    Main CLI for managing FlowGuard services and utilities.
    """
    pass


@cli.command()
@click.option('--host', default='0.0.0.0', help='Host to bind to')
@click.option('--port', default=8000, help='Port to bind to')
@click.option('--reload', is_flag=True, help='Enable auto-reload')
def start_gateway(host: str, port: int, reload: bool):
    """Start the Events Gateway service"""
    import uvicorn
    
    click.echo("="*50)
    click.echo("Starting FlowGuard Events Gateway")
    click.echo("="*50)
    click.echo(f"Host: {host}")
    click.echo(f"Port: {port}")
    click.echo(f"Reload: {reload}")
    click.echo("="*50)
    
    uvicorn.run(
        "src.services.events_gateway.main:app",
        host=host,
        port=port,
        reload=reload,
        log_level="info"
    )


@cli.command()
def health_check():
    """Check health of all FlowGuard components"""
    from src.services.events_gateway.producers.kafka_producer import check_kafka_connection
    
    click.echo("="*50)
    click.echo("FlowGuard Health Check")
    click.echo("="*50)
    
    # Check Kafka
    click.echo("\nKafka Cluster:")
    if check_kafka_connection():
        click.secho("  ✓ Connected", fg='green')
    else:
        click.secho("  ✗ Not connected", fg='red')
    
    click.echo("\n" + "="*50)


@cli.command()
@click.option('--rate', default=1, help='Events per second')
@click.option('--duration', default=10, help='Duration in seconds')
@click.option('--event-type', type=click.Choice(['orders', 'clicks', 'both']), default='both')
def simulate(rate: int, duration: int, event_type: str):
    """Simulate event generation for testing
    
    Generate fake events and send them to the Events Gateway.
    """
    click.echo("="*50)
    click.echo("Event Simulator")
    click.echo("="*50)
    click.echo(f"Rate: {rate} events/sec")
    click.echo(f"Duration: {duration} seconds")
    click.echo(f"Event Type: {event_type}")
    click.echo("="*50)
    click.echo("\n⚠  Simulator implementation coming in next phase...")
    click.echo("For now, use curl or Postman to send events to:")
    click.echo("  - POST http://localhost:8000/api/v1/orders")
    click.echo("  - POST http://localhost:8000/api/v1/clicks")


if __name__ == "__main__":
    cli()



