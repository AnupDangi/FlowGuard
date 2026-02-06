"""Kafka consumer to monitor events in real-time."""
import json
import signal
import sys
from datetime import datetime

from confluent_kafka import Consumer, KafkaException
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:19092,localhost:19093,localhost:19094',
    'group.id': 'monitor-consumer',
    'auto.offset.reset': 'latest',  # Only read new messages
    'enable.auto.commit': True,
}

# Topics to monitor
TOPICS = ['raw.orders.v1', 'raw.clicks.v1']

running = True


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully."""
    global running
    print(f"\n{Fore.YELLOW}⚠️  Shutting down monitor...{Style.RESET_ALL}")
    running = False


def format_timestamp(ts):
    """Format timestamp for display."""
    return datetime.fromtimestamp(ts / 1000).strftime('%H:%M:%S')


def print_event(topic, key, value, timestamp):
    """Print event in a nice format."""
    try:
        event_data = json.loads(value)
        
        # Topic header
        if topic == 'raw.orders.v1':
            print(f"\n{Fore.GREEN}{'='*80}{Style.RESET_ALL}")
            print(f"{Fore.GREEN}📦 ORDER EVENT{Style.RESET_ALL} | {Fore.CYAN}{format_timestamp(timestamp)}{Style.RESET_ALL}")
            print(f"{Fore.GREEN}{'='*80}{Style.RESET_ALL}")
            
            # Order details
            print(f"{Fore.YELLOW}Event ID:{Style.RESET_ALL} {event_data.get('event_id', 'N/A')}")
            print(f"{Fore.YELLOW}User ID:{Style.RESET_ALL} {event_data.get('user_id', 'N/A')}")
            print(f"{Fore.YELLOW}Order ID:{Style.RESET_ALL} {event_data.get('order_id', 'N/A')}")
            print(f"{Fore.YELLOW}Item:{Style.RESET_ALL} {event_data.get('item_name', 'N/A')} (ID: {event_data.get('item_id', 'N/A')})")
            print(f"{Fore.YELLOW}Price:{Style.RESET_ALL} ₹{event_data.get('price', 0)}")
            print(f"{Fore.YELLOW}Timestamp:{Style.RESET_ALL} {event_data.get('timestamp', 'N/A')}")
            
        elif topic == 'raw.clicks.v1':
            print(f"\n{Fore.BLUE}{'='*80}{Style.RESET_ALL}")
            print(f"{Fore.BLUE}👆 CLICK EVENT{Style.RESET_ALL} | {Fore.CYAN}{format_timestamp(timestamp)}{Style.RESET_ALL}")
            print(f"{Fore.BLUE}{'='*80}{Style.RESET_ALL}")
            
            # Click details
            print(f"{Fore.YELLOW}Event ID:{Style.RESET_ALL} {event_data.get('event_id', 'N/A')}")
            print(f"{Fore.YELLOW}User ID:{Style.RESET_ALL} {event_data.get('user_id', 'N/A')}")
            print(f"{Fore.YELLOW}Ad ID:{Style.RESET_ALL} {event_data.get('ad_id', 'N/A')}")
            print(f"{Fore.YELLOW}Is Click:{Style.RESET_ALL} {event_data.get('is_click', False)}")
            print(f"{Fore.YELLOW}Session ID:{Style.RESET_ALL} {event_data.get('session_id', 'N/A')}")
        
        print(f"{Fore.CYAN}{'─'*80}{Style.RESET_ALL}\n")
        
    except json.JSONDecodeError:
        print(f"{Fore.RED}❌ Failed to decode event{Style.RESET_ALL}")


def main():
    """Main monitoring loop."""
    global running
    
    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    # Create consumer
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(TOPICS)
    
    print(f"{Fore.GREEN}{'='*80}{Style.RESET_ALL}")
    print(f"{Fore.GREEN}🔍 FlowGuard Event Monitor{Style.RESET_ALL}")
    print(f"{Fore.GREEN}{'='*80}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}Monitoring topics:{Style.RESET_ALL} {', '.join(TOPICS)}")
    print(f"{Fore.CYAN}Kafka:{Style.RESET_ALL} localhost:19092-19094")
    print(f"{Fore.YELLOW}Press Ctrl+C to stop{Style.RESET_ALL}\n")
    
    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                raise KafkaException(msg.error())
            
            # Print the event
            print_event(
                topic=msg.topic(),
                key=msg.key().decode('utf-8') if msg.key() else None,
                value=msg.value().decode('utf-8'),
                timestamp=msg.timestamp()[1]
            )
    
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"{Fore.GREEN}✅ Monitor stopped{Style.RESET_ALL}")


if __name__ == "__main__":
    main()
