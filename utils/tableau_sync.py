import tableauserverclient as TSC
import pandas as pd
import streamlit as st
import tempfile
import os

def trigger_tableau_report(df, report_name, project_name="Foundry Analytics"):
    """
    Publishes the dataframe to Tableau Cloud as a Parquet Datasource.
    Satisfies: Only tds, tdsx, tde, hyper, parquet files can be published.
    """
    TOKEN_NAME = st.secrets.get("TABLEAU_TOKEN_NAME", "foundry_token")
    TOKEN_VALUE = st.secrets.get("TABLEAU_TOKEN_VALUE", "YOUR_TOKEN")
    SITE_ID = st.secrets.get("TABLEAU_SITE_ID", "your_site")
    SERVER_URL = st.secrets.get("TABLEAU_SERVER_URL", "https://prod-useast-a.online.tableau.com")

    try:
        tableau_auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_VALUE, SITE_ID)
        server = TSC.Server(SERVER_URL, use_server_version=True)

        with server.auth.sign_in(tableau_auth):
            # 1. Project Lookup
            all_projects, pagination = server.projects.get()
            project = next((p for p in all_projects if p.name == project_name), None)
            
            # Fallback to 'Default' if your specific project is missing
            if not project:
                project = next((p for p in all_projects if p.name == "Default"), all_projects[0])

            # 2. Create a Temporary Parquet File
            # Parquet is a valid extension listed in the error message
            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                df.to_parquet(tmp.name, index=False)
                tmp_path = tmp.name

            try:
                # 3. Publish as a Datasource
                # Note: We use server.datasources, not server.workbooks
                new_datasource = TSC.DatasourceItem(project.id, name=report_name)
                
                server.datasources.publish(
                    new_datasource, 
                    tmp_path, 
                    TSC.Server.PublishMode.Overwrite
                )
                return True, f"Success: Pushed to {project.name}"
            
            finally:
                # 4. Clean up temporary file
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

    except Exception as e:
        return False, str(e)
