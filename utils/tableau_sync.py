import tableauserverclient as TSC
import pandas as pd
import streamlit as st
import tempfile
import os

def trigger_tableau_report(df, report_name, project_name="Foundry Analytics"):
    """
    Core function: Publishes a single dataframe to Tableau Cloud.
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

            # 2. Create Temporary Parquet File
            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                df.to_parquet(tmp.name, index=False)
                tmp_path = tmp.name

            try:
                # 3. Publish as a Datasource
                new_datasource = TSC.DatasourceItem(project.id, name=report_name)
                server.datasources.publish(
                    new_datasource, 
                    tmp_path, 
                    TSC.Server.PublishMode.Overwrite
                )
                return True, f"Successfully updated {report_name} in {project.name}"
            
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

    except Exception as e:
        return False, f"Tableau Error: {str(e)}"

def sync_global_data(data_map, master_report_name="Foundry_Global_Master"):
    """
    New Feature: Combines multiple region dataframes into one single push.
    'data_map' should be a dict: {"APAC": df1, "LATAM": df2, "EMEA": df3}
    """
    combined_list = []
    
    for region_name, df in data_map.items():
        if df is not None and not df.empty:
            # Ensure each row knows its origin region before stacking
            temp_df = df.copy()
            temp_df['Origin_Region'] = region_name
            combined_list.append(temp_df)
    
    if not combined_list:
        return False, "No data available to sync."

    # Stack all regions vertically (The "Union" done in code)
    global_df = pd.concat(combined_list, ignore_index=True)
    
    # Push the unified master file
    return trigger_tableau_report(global_df, master_report_name)
