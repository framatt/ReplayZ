import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Text, Float, JSON
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Configure SQLAlchemy for replication task storage
DATABASE_URL = 'sqlite:///jobs.sqlite'
engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Define the ReplicationTask model
class ReplicationTask(Base):
    __tablename__ = 'replication_tasks'

    source_host_id = Column(String, primary_key=True)
    dest_host_id = Column(String)
    dest_host_name = Column(String)
    status = Column(String)
    message = Column(Text)
    start_time = Column(Float)
    first_history_timestamp = Column(Integer)
    # Store Zabbix API connection details for this task
    source_url = Column(String)
    source_token = Column(String)
    dest_url = Column(String)
    dest_token = Column(String)
    # Store history and item_mapping as JSON
    history = Column(JSON)
    item_mapping = Column(JSON)
    last_sent_index = Column(JSON)
    cycle_offset = Column(Integer, default=0)
    progress = Column(Float, default=0.0)

# Create the table if it doesn't exist
Base.metadata.create_all(engine)

# Create a session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Configuration dictionary
config = {
    "source_url": os.getenv('SOURCE_ZABBIX_URL', ""),
    "source_token": os.getenv('SOURCE_ZABBIX_TOKEN', ""),
    "dest_url": os.getenv('DEST_ZABBIX_URL', ""),
    "dest_token": os.getenv('DEST_ZABBIX_TOKEN', ""),
    "dest_trapper_host": os.getenv('DEST_TRAPPER_HOST', ""),
    "dest_trapper_port": int(os.getenv('DEST_TRAPPER_PORT', "10051")),
    "replay_duration_hours": int(os.getenv('REPLAY_DURATION_HOURS', "24")) # New config for replay duration
}
