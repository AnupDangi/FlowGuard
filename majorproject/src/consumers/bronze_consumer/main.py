"""Bronze Consumer Entry Point

Main script to run the Kafka → Snowflake consumer.
Handles graceful shutdown and logging setup.
"""

import sys
import signal
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.consumers.bronze_consumer.consumer import BronzeConsumer
from src.consumers.bronze_consumer.config import get_config
from src.consumers.bronze_consumer.snowflake_writer import close_writer

# Global consumer instance for signal handling
_consumer: BronzeConsumer = None


def setup_logging():
    """Configure logging"""
    config = get_config()
    
    logging.basicConfig(
        level=config.log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('/tmp/bronze_consumer.log')
        ]
    )


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger = logging.getLogger(__name__)
    logger.info(f"Received signal {signum}, initiating shutdown...")
    
    global _consumer
    if _consumer:
        _consumer.stop()
    
    sys.exit(0)


def main():
    """Main entry point"""
    global _consumer
    
    # Setup
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logger.info("="*60)
        logger.info("FlowGuard Bronze Consumer Starting")
        logger.info("="*60)
        
        # Test Snowflake connection
        from src.consumers.bronze_consumer.snowflake_writer import get_writer
        writer = get_writer()
        if not writer.test_connection():
            logger.error("Snowflake connection test failed. Exiting.")
            sys.exit(1)
        
        # Create and start consumer
        _consumer = BronzeConsumer()
        logger.info("Consumer initialized successfully")
        
        # Start consuming (blocking)
        _consumer.start()
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Cleanup
        logger.info("Cleaning up...")
        if _consumer:
            _consumer.stop()
        close_writer()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    main()
