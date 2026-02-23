import streamlit as st
import chromadb
import pandas as pd
from sentence_transformers import SentenceTransformer

# Initialize embedding model
@st.cache_resource
def get_embedding_model():
    return SentenceTransformer('all-MiniLM-L6-v2')

def build_schema_index():
    """Create a Chroma collection with descriptions of all tables and columns."""
    chroma_client = chromadb.PersistentClient(path="./chroma_db")
    collection = chroma_client.get_or_create_collection(name="schema_index")
    
    # Define your schema with descriptions
    schema_docs = [
        {"text": "content_planning table: stores planned content for networks. Columns: network (e.g., MAX US), content_title, status (Scheduled, Fulfilled, Delivered, Not Ready), planned_date, region (NA, APAC, etc.)"},
        {"text": "work_orders table: tracks work orders. Columns: work_order (WO-xxx), offering (e.g., MAX NA - Encoding), status (Delayed, In Progress, Completed), due_date, region, vendor, priority"},
        {"text": "deals table: content deals. Columns: deal_name, vendor, deal_value, deal_date, region, status (Active, Completed, Pending)"},
    ]
    
    # If collection is empty, add embeddings
    if collection.count() == 0:
        emb_model = get_embedding_model()
        embeddings = emb_model.encode([d["text"] for d in schema_docs]).tolist()
        collection.add(
            documents=[d["text"] for d in schema_docs],
            embeddings=embeddings,
            ids=[f"doc_{i}" for i in range(len(schema_docs))]
        )
    return collection

def retrieve_relevant_schema(question, top_k=3):
    """Retrieve most relevant schema descriptions for the question."""
    collection = build_schema_index()
    emb_model = get_embedding_model()
    q_emb = emb_model.encode([question]).tolist()
    results = collection.query(query_embeddings=q_emb, n_results=top_k)
    return results['documents'][0]  # list of relevant schema texts
import chromadb
import pandas as pd
from sentence_transformers import SentenceTransformer

# Initialize embedding model
@st.cache_resource
def get_embedding_model():
    return SentenceTransformer('all-MiniLM-L6-v2')

def build_schema_index():
    """Create a Chroma collection with descriptions of all tables and columns."""
    chroma_client = chromadb.PersistentClient(path="./chroma_db")
    collection = chroma_client.get_or_create_collection(name="schema_index")
    
    # Define your schema with descriptions
    schema_docs = [
        {"text": "content_planning table: stores planned content for networks. Columns: network (e.g., MAX US), content_title, status (Scheduled, Fulfilled, Delivered, Not Ready), planned_date, region (NA, APAC, etc.)"},
        {"text": "work_orders table: tracks work orders. Columns: work_order (WO-xxx), offering (e.g., MAX NA - Encoding), status (Delayed, In Progress, Completed), due_date, region, vendor, priority"},
        {"text": "deals table: content deals. Columns: deal_name, vendor, deal_value, deal_date, region, status (Active, Completed, Pending)"},
        # Add more detailed descriptions for each column
    ]
    
    # If collection is empty, add embeddings
    if collection.count() == 0:
        emb_model = get_embedding_model()
        embeddings = emb_model.encode(schema_docs).tolist()
        collection.add(
            documents=[d["text"] for d in schema_docs],
            embeddings=embeddings,
            ids=[f"doc_{i}" for i in range(len(schema_docs))]
        )
    return collection

def retrieve_relevant_schema(question, top_k=3):
    """Retrieve most relevant schema descriptions for the question."""
    collection = build_schema_index()
    emb_model = get_embedding_model()
    q_emb = emb_model.encode([question]).tolist()
    results = collection.query(query_embeddings=q_emb, n_results=top_k)
    return results['documents'][0]  # list of relevant schema texts
