import streamlit as st
import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
import json
import uuid

# Kafka configuration
KAFKA_TOPIC = "aml_alerts"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # Use service name if using Docker Compose


# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to send data to Kafka
def send_to_kafka(data):
    producer.send(KAFKA_TOPIC, data)
    producer.flush()

# Streamlit app
st.title("Real-time AML Alerts System")

# File uploader
uploaded_file = st.file_uploader("Choose an Excel file", type="xlsx")
if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.write(df)

    # Send data to Kafka
    for index, row in df.iterrows():
        transaction = row.to_dict()
        transaction["transaction_id"] = str(uuid.uuid4())
        send_to_kafka(transaction)

# Kafka consumer setup
consumer = KafkaConsumer(KAFKA_TOPIC,
                         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='aml_alerts_group')

st.header("Real-time Alerts")

for message in consumer:
    alert = message.value
    if 'transaction_id' in alert:
        st.json(alert)
        st.button("Acknowledge", key=alert['transaction_id'])
    else:
        st.error("Error: 'transaction_id' not found in alert")
