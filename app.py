import streamlit as st
import pandas as pd
import altair as alt
from kafka import KafkaProducer
import json
import uuid

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to send data to Kafka
def send_to_kafka(data):
    data["transaction_id"] = str(uuid.uuid4())  # Adding a unique transaction ID
    producer.send('aml_alerts', value=data)
    producer.flush()

# Streamlit app interface
st.title('AML Alerts')

uploaded_file = st.file_uploader("Choose an Excel file", type="xlsx")

if uploaded_file is not None:
    df = pd.read_excel(uploaded_file)
    st.write("Data preview:")
    st.write(df.head())

    for _, row in df.iterrows():
        send_to_kafka(row.to_dict())
    
    st.write("Data sent to Kafka!")

    # Example of using Altair to create a chart
    chart = alt.Chart(df).mark_bar().encode(
        x='Merchant Name',
        y='Available Balance'
    )
    st.altair_chart(chart, use_container_width=True)
