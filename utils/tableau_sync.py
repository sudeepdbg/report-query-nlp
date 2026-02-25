import tableauserverclient as TSC
import pandas as pd
import streamlit as st
import tempfile
import os
import time

def trigger_tableau_report(df, report_name, project_name="Foundry Analytics"):
    """
    Publishes the current dataframe as a Data Source to Tableau Cloud.
    Fixes the 'File path' error by using a physical temporary file.
    """
    # 1. Credentials from Streamlit Secrets
    TOKEN_NAME = st.secrets.get("TABLEAU_TOKEN_NAME", "foundry_token")
    TOKEN_VALUE = st.secrets.get("TABLEAU_TOKEN_VALUE", "YOUR_TOKEN")
    SITE_ID = st.secrets.get("TABLEAU_SITE_ID", "your_site")
    SERVER_URL = st.secrets.get("TABLEAU_SERVER_URL", "https://prod-useast-a.online.tableau.com")

    try:
        tableau_auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_VALUE, SITE_ID)
        server = TSC.Server(SERVER_URL, use_server_version=True)

        with server.auth.sign_in(tableau_auth):
            # 2. Robust Project Lookup
            all_projects, pagination = server.projects.get()
            project = next((p for p in all_projects if p.name == project_name), None)
            
            # Fallback: If 'Foundry Analytics' doesn't exist, use 'Default'
            if not project:
                project = next((p for p in all_projects if p.name == "Default"), all_projects[0])
                project_name = project.name

            # 3. Create a PHYSICAL Temporary File
            # Tableau's publish method requires a string path to an existing file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                df.to_csv(tmp.name, index=False)
                tmp_path = tmp.name

            try:
                # 4. Publish to Tableau
                new_datasource = TSC.DatasourceItem(project.id, name=report_name)
                
                # We pass the tmp_path (the string path), NOT the buffer
                server.datasources.publish(
                    new_datasource, 
                    tmp_path, 
                    TSC.Server.PublishMode.Overwrite
                )
                
                return True, f"Pushed to Tableau Project: {project_name}"
            
            finally:
                # 5. Clean up the temp file after upload
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

    except Exception as e:
        return False, f"Tableau Error: {str(e)}"
