import time
import streamlit as st
import pandas as pd

# import gspread

from snowflake.snowpark.context import get_active_session


def quote_identifier(identifier):
    return f"'{identifier}'"


session = get_active_session()


tab1, tab2 = st.tabs(["My Pipelines", "Create New Piplines"])

with tab1:
    st.title("My Pipelines")

    display_query = "SELECT P_ID, SPREADSHEET_ID, SHEET_NAME, TABLE_NAME, PIPELINE_NAME FROM GSHEETS_PIPELINE"

    display_result = session.sql(display_query)
    st.dataframe(display_result, use_container_width=True)

    # pipe_id = ""
    # pipe_id = st.text_input(label="Enter pipe id")
    edit_tab1, delete_tab2 = st.tabs(["Edit Pipeline", "Delete Pipeline"])
    with edit_tab1:
        with st.form("Edit pipe id form", clear_on_submit=True):
            edit_pipe_id = st.text_input(label="Enter pipe id", key="edit text box")
            edit_btn = st.form_submit_button("Edit pipe")
            if edit_btn:
                pass
        if edit_pipe_id and edit_pipe_id != "":
            pipe_query = f"SELECT * FROM GSHEETS_PIPELINE where P_ID = '{edit_pipe_id}'"
            pipe = session.sql(pipe_query).collect()
            pipe = pd.DataFrame(pipe)
            if len(pipe) == 0:
                st.error("Enter a valid pipe id")
            else:
                p_spreadsheet_id = pipe["SPREADSHEET_ID"][0]
                p_sheet_name = pipe["SHEET_NAME"][0]
                p_auth_secret = pipe["AUTH_SECRET"][0]
                p_table_name = pipe["TABLE_NAME"][0]
                p_pipe_name = pipe["PIPELINE_NAME"][0]
                p_freq = pipe["UPDATE_FREQ"][0]
                st.subheader("Edit GSheet-Snowflake Pipeline")

                with st.form("Edit pipe form", clear_on_submit=True):
                    col1, col2 = st.columns(2)
                    p_spreadsheet_id = col1.text_input(
                        "Spreadsheet ID:", p_spreadsheet_id
                    )

                    p_sheet_name = col2.text_input("Sheet Name:", p_sheet_name)

                    p_auth_secret = col1.text_input(
                        "Authentication Secret:", p_auth_secret
                    )

                    frequency_options = [
                        "1 min",
                        "10 min",
                        "30 min",
                        "Daily",
                        "Weekly",
                        "Monthly",
                    ]

                    p_time_frequency = col2.selectbox(
                        "Time Frequency", frequency_options, index=3
                    )

                    st.write("You have selected time frequency of :", p_time_frequency)

                    p_table_name = col1.text_input("Table Name:", p_table_name)

                    p_pipe_name = col2.text_input("Pipeline Name:", p_pipe_name)

                    submitted = st.form_submit_button("Edit Pipe")
                    if submitted:
                        st.write("PipeLine Edited Successfully !")
                        update_pipe_query = f"""
                            UPDATE GSHEETS_PIPELINE
                            SET SPREADSHEET_ID={quote_identifier(p_spreadsheet_id)},
                            SHEET_NAME={quote_identifier(p_sheet_name)},
                            AUTH_SECRET={quote_identifier(p_auth_secret)},
                            TABLE_NAME={quote_identifier(p_table_name)},
                            PIPELINE_NAME={quote_identifier(p_pipe_name)},
                            UPDATE_FREQ={quote_identifier(p_time_frequency)}
                            WHERE P_ID = {edit_pipe_id}
                        """
                        temp = session.sql(update_pipe_query).collect()
                        st.experimental_rerun()
    with delete_tab2:
        with st.form("Delete pipe id form", clear_on_submit=True):
            delete_pipe_id = st.text_input(label="Enter pipe id", key="delete text box")
            delete_btn = st.form_submit_button("Delete pipe")
            if delete_btn:
                pass
        if delete_pipe_id:
            pipe_query = (
                f"SELECT * FROM GSHEETS_PIPELINE where P_ID = '{delete_pipe_id}'"
            )
            pipe = session.sql(pipe_query).collect()
            pipe = pd.DataFrame(pipe)
            if len(pipe) == 0:
                st.error("Enter a valid pipe id")
                pipe_id = ""
            else:
                st.write(
                    f"Are you sure to delete the pipeline with P_ID = {delete_pipe_id}"
                )
                if st.button("Yes", key="Yes"):
                    delete_pipe_query = (
                        f"DELETE FROM GSHEETS_PIPELINE where P_ID = '{delete_pipe_id}'"
                    )
                    session.sql(delete_pipe_query).collect()
                    st.write("Pipe deleted")
                    del delete_pipe_id
                    st.experimental_rerun()
                if st.button("No", key="No"):
                    del delete_pipe_id
                    st.experimental_rerun()


with tab2:
    st.title("Snowflake Pipeline Setup")

    # Create a form within the tab
    with st.form("Pipe form", clear_on_submit=True):
        # Divide the form into two columns
        col1, col2 = st.columns(2)

        spreadsheet_id = col1.text_input("Spreadsheet ID:")

        sheet_name = col2.text_input("Sheet Name:")

        # Common Fields
        auth_secret = col1.text_input("Authentication Secret:")
        
        selected_warehouse = st.text_input("Warehouse:", value="COMPUTE_WH", disabled=True)

        selected_database = st.text_input("Database:", value="GSHEETS_SNOWFLAKE_APP", disabled=True)

        selected_schema = st.text_input("Schema:", value="APP_INSTANCE_SCHEMA", disabled=True)

        # Column 1: Time Frequency Dropdown
        frequency_options = ["1 min", "10 min", "30 min", "Daily", "Weekly", "Monthly"]
        time_frequency = col2.selectbox("Time Frequency", frequency_options)

        # Column 2: Display selected time frequency
        #st.write("You have selected time frequency of:", time_frequency)

        # Column 1: Table Name
        table_name = col1.text_input("Table Name:")

        # Column 2: Pipeline Name
        pipeline_name = col2.text_input("Pipeline Name:")

        f_submitted = st.form_submit_button("Create Pipe")
        if f_submitted:
            if (
                spreadsheet_id
                and sheet_name
                and auth_secret
                and frequency_options
                and table_name
                and pipeline_name
            ):
                st.write("PipeLine Created Successfully !")
                insert_query = f"""

                    INSERT INTO GSHEETS_PIPELINE(SPREADSHEET_ID, SHEET_NAME, AUTH_SECRET, WAREHOUSE_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, PIPELINE_NAME, UPDATE_FREQ)

                    VALUES ({quote_identifier(spreadsheet_id)}, {quote_identifier(sheet_name)}, {quote_identifier(auth_secret)}, {quote_identifier(selected_warehouse)}, {quote_identifier(selected_database)}, {quote_identifier(selected_schema)}, {quote_identifier(table_name)}, {quote_identifier(pipeline_name)}, {quote_identifier(time_frequency)})

                """
                result = session.sql(insert_query).collect()
                st.experimental_rerun()
            else:
                st.error("Enter all fields")