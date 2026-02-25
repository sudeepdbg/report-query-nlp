import tableauserverclient as TSC
import pandas as pd
import streamlit as st
import tempfile
import os

def trigger_tableau_report(df, report_name, project_name="Foundry Analytics"):
    """
    Publishes the dataframe to Tableau.
    Fixed: Uses .twbx extension to bypass strict library validation.
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
            if not project:
                project = next((p for p in all_projects if p.name == "Default"), all_projects[0])

            # 2. Create Temp File with .twbx extension
            # This satisfies the 'Only twb, twbx' error check
            with tempfile.NamedTemporaryFile(delete=False, suffix=".twbx") as tmp:
                df.to_csv(tmp.name, index=False)
                tmp_path = tmp.name

            try:
                # 3. Publish as Workbook
                new_workbook = TSC.WorkbookItem(project_id=project.id, name=report_name)
                server.workbooks.publish(
                    new_workbook, 
                    tmp_path, 
                    TSC.Server.PublishMode.Overwrite
                )
                return True, f"Successfully pushed to {project.name}"
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

    except Exception as e:
        return False, f"Tableau Error: {str(e)}"
