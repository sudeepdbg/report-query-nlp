import tableauserverclient as TSC
import pandas as pd
import streamlit as st
import tempfile
import os

def trigger_tableau_report(df, report_name, project_name="Foundry Analytics"):
    """
    Publishes data to Tableau Cloud. 
    Uses .csv extension but publishes as a DatasourceItem to avoid workbook validation errors.
    """
    # Credentials from Streamlit Secrets
    TOKEN_NAME = st.secrets.get("TABLEAU_TOKEN_NAME", "foundry_token")
    TOKEN_VALUE = st.secrets.get("TABLEAU_TOKEN_VALUE", "YOUR_TOKEN")
    SITE_ID = st.secrets.get("TABLEAU_SITE_ID", "your_site")
    SERVER_URL = st.secrets.get("TABLEAU_SERVER_URL", "https://prod-useast-a.online.tableau.com")

    try:
        tableau_auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_VALUE, SITE_ID)
        server = TSC.Server(SERVER_URL, use_server_version=True)

        with server.auth.sign_in(tableau_auth):
            # 1. Project Discovery
            all_projects, pagination = server.projects.get()
            project = next((p for p in all_projects if p.name == project_name), None)
            
            # Fallback to Default if the specific project isn't found
            if not project:
                project = next((p for p in all_projects if p.name == "Default"), all_projects[0])

            # 2. Create a temporary CSV
            # Most Tableau Cloud sites accept CSV uploads via the REST API 
            # when handled as a DatasourceItem.
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                df.to_csv(tmp.name, index=False)
                tmp_path = tmp.name

            try:
                # 3. Publish as a DATASOURCE (not a workbook)
                # This avoids the 'Invalid twb/twbx' error from your screenshot
                new_datasource = TSC.DatasourceItem(project.id, name=report_name)
                
                # We use datasources.publish
                server.datasources.publish(
                    new_datasource, 
                    tmp_path, 
                    TSC.Server.PublishMode.Overwrite
                )
                return True, f"Success: Pushed to {project.name}"
            
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

    except Exception as e:
        return False, str(e)
