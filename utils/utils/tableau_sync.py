import tableauserverclient as TSC
import pandas as pd
import io
import streamlit as st

def trigger_tableau_report(df, report_name, project_name="Foundry Analytics"):
    """
    Publishes the current dataframe as a Data Source to Tableau.
    """
    # Use st.secrets for production security
    TOKEN_NAME = st.secrets.get("TABLEAU_TOKEN_NAME", "foundry_token")
    TOKEN_VALUE = st.secrets.get("TABLEAU_TOKEN_VALUE", "YOUR_ACTUAL_TOKEN")
    SITE_ID = st.secrets.get("TABLEAU_SITE_ID", "your_site")
    SERVER_URL = st.secrets.get("TABLEAU_SERVER_URL", "https://prod-useast-a.online.tableau.com")

    try:
        tableau_auth = TSC.PersonalAccessTokenAuth(TOKEN_NAME, TOKEN_VALUE, SITE_ID)
        server = TSC.Server(SERVER_URL, use_server_version=True)

        with server.auth.sign_in(tableau_auth):
            all_projects, pagination = server.projects.get()
            project = next((p for p in all_projects if p.name == project_name), None)
            
            if not project:
                return False, f"Project '{project_name}' not found."

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            new_datasource = TSC.DatasourceItem(project.id, name=report_name)
            server.datasources.publish(new_datasource, csv_buffer.getvalue(), 'Overwrite')
            
            return True, f"Successfully pushed to Tableau project: {project_name}"

    except Exception as e:
        return False, str(e)
