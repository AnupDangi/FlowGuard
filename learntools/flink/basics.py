from pyflink.datastream import StreamExecutionEnvironment

## Concept1: Execution Environment
## create execution environment
env = StreamExecutionEnvironment.get_execution_environment()


## Note : Apache Flink is a deterministic state machine evolving over event time, with fault-tolerant memory.

env.execute("my_flink_job")


## streams are typed 
## every stream has a logical type

from pyflink.common.typeinfo import Types

## Concept2: Typed Streams
stream=env.from_collection(
    [
        (1,"order"),(2,"order")
    ],
    type_info=Types.TUPLE([Types.INT(),Types.STRING()]) ## types for the stream
)


## Concept3: Parallelism and Keyed Streams
## parallelism is not thread in flink 
## parallelism is number of task slots
## independent operator instances
## each with its own state



env.set_parallelism(4) ## set parallelism at env level


## keyed streams=distributed memory 
keyed=stream.key_by(lambda x:x[0]) ## key by first element

## without key_by flink is stateless

## Flink becomes a memory machine.



## Concept4: State is explict and scoped
## state lives inside functions ,not globals

## key concepts
## State is checkpointed
## State is per key
## State survives failures
## You never manage persistence yourself

from pyflink.datastream import KeyedProcessFunction

class CountFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        # Initialize state
        self.count_state = runtime_context.get_state(
            name="count_state",
            state_type=Types.LONG()
        )

    def process_element(self, value, ctx):
        # Update and emit state
        current_count = self.count_state.value()
        if current_count is None:
            current_count = 0
        current_count += 1
        self.count_state.update(current_count)
        yield (value[0], current_count)


## Concept 5: Time does not exist unless you define it.

## By default flink runs on processing time 
## to do real streaming you must use
## exact timestamp
## assign watermarks

from pyflink.common.watermark_strategy import WatermarkStrategy
from datetime import timedelta

watermarks = WatermarkStrategy \
    .for_bounded_out_of_orderness(timedelta(seconds=5)) \
    .with_timestamp_assigner(lambda e, ts: e[2])


## without this we cannot 
## do time based operations like windows
## late data is silently dropped
## metrics drift
## Time must declared.


## Concept 6: Windows are state machines

from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time

windowed=keyed.window(
                        TumblingEventTimeWindows.of(Time.minutes(1))
                    )

##  internally state allocated per key per windows
## timers are scheduled
## cleanup triggered by watermarks


## Concept 7: fault tolerance is automatic,not optional
# Flink checkpoints:
    # operator state
    # keyed state
    # stream positions

env.enable_checkpointing(10000) ## checkpoint every 10 seconds


## Concept 8: Stateless vs Stateful in Flink

"""
    Stateless Processing: 
        - Each event is processed independently without relying on any stored state.
        - Examples: simple transformations like map, filter.

    Stateful Processing:
        - Processing depends on previously stored state information.
        - Examples: aggregations, windowed computations, joins.
"""

## Stateless : map

stream.map(lambda e: (e["user"], e["amount"]))
## No memory,no history,no context
## if flink creashes and  nothing will be restored
## Examples: function to parse JSON, filtering invalid records,unit conversions

## Stateful : keyed + window + reduce
## Memory is scoped to - key ,windows ,operators

## conceputal example of stateful processing

## think like incrementing a user count

## just pseudocode
state={}
state["user_id"].count += 1

## it is very important to understand stateful processing in flink
## because it is the core of flink's power
 ## it enables complex event processing, real-time analytics, and more
 ## eg transactions counters,rolling averages,last-seen timestamps,fraud risk scores
## Fraud detection is stateful computation:

## a keyed stream is powereful by adding keyed state 
## state is isolated per state
## no race condition
 

## stream.key_by(lambda e: e["user_id"])



## Sink in Flink 
## sinks are also stateful
## sinks maintain state about what has been written
## this is important for exactly-once semantics
## eg Sink = write to database but 

## What can Sink do ? 
"""
    - Write to external systems (databases, files, message queues)
    - Support different delivery guarantees (at-most-once, at-least-once, exactly once 
    - Block until data is acknowledged by the external system
    - Trigger an alert
"""

## MySink is pseudocode

class MySink:
    """ Write to external system with exactly-once semantics """
    pass


stream.add_sink(MySink())


## Types of Sink levels of guarantees
## At-most-once: fast, unsafe
## At-least-once: duplicates possible
## Exactly-once: requires coordination
## Flink provides exactly-once semantics with two-phase commit protocol


## I think this is enough for basics of flink to get started later you can explore
