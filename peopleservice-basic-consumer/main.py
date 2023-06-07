import os
import json
import logging

from dotenv import load_dotenv
from kafka.consumer import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

load_dotenv()


key_deserializer = lambda key: key.decode('utf-8')
value_deserializer = lambda val : json.loads(val.decode('utf-8'))

def main():
    logger.info(f"""
    Starting python Consumer for 
    {os.environ['TOPICS_PEOPLE_BASIC_NAME']}
    """)

    consumer = KafkaConsumer(
        bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
        group_id=os.environ['CONSUMER_GROUP'],
        key_deserializer=key_deserializer,
        value_deserializer=value_deserializer
    )


    consumer.subscribe([os.environ['TOPICS_PEOPLE_BASIC_NAME']])

    for record in consumer:
        logger.info(f"""
        Consumed person {record.value}
        with key '{record.key}'
        from partition {record.partition}
        with offset {record.offset}
        """)



if __name__ == '__main__':
    main()