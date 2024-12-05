import streamlit as st
import pandas as pd
import snowflake.connector
import logging
from pyxlsb import open_workbook
from st_aggrid import AgGrid, GridOptionsBuilder, DataReturnMode, GridUpdateMode, JsCode
from streamlit_autorefresh import st_autorefresh


selected_views = []
st.set_page_config(page_title="Snowflake View Validation", layout="wide")
st.title("Snowflake View Validation")

logging.basicConfig(
    filename="snowflake_view_execution.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
with open("snowflake_view_execution.log", "w"):
    pass
  
# st_autorefresh(interval=10000, key="dataframe_autorefresh")

# viewDf = pd.read_csv(r'C:\Users\ecorjoh\OneDrive - Ericsson\Projects\Anupama_Chandra\view_validation.csv')
# viewList_All = viewDf['view_name'].to_list()
# viewList_Error = viewDf[viewDf['status'] == 'ERROR']['view_name'].to_list()

# Initialize all session states to default value
if 'schemas' not in st.session_state:
  st.session_state['schemas'] = []
if 'views' not in st.session_state:
  st.session_state['views'] = []
if 'df' not in st.session_state:
  st.session_state['df'] = pd.DataFrame(columns=['view_name', 'row_count','status', 'error'])
if 'selected_views' not in st.session_state:
  st.session_state['selected_views'] = []
       
def connect_to_snowflake(user):
  try:
    with st.spinner("Connecting to Snowflake"):
      sf_conn = snowflake.connector.connect(
      user=user.upper(),
      account='htb40669.us-east-1',
      authenticator='externalbrowser',
      role='DP_IBS_MANA_NTW_ETL_QA',
      warehouse='WH_NTW_QA',
      database='IBS_MANA_QA',
      # schema=schema.upper()
      )
      
      st.session_state["sf_conn"] = sf_conn
      st.session_state["sf_cursor"] = sf_conn.cursor()
        
      st.success("Connected to Snowflake!")
      
      fetch_schemas(st.session_state["sf_cursor"])
  except Exception as e:
    st.error(f"Failed to connect to Snowflake: {e}")

 
def fetch_views(schema, sf_cursor):
  query = f"""
  SELECT TABLE_NAME
  FROM INFORMATION_SCHEMA.VIEWS
  WHERE TABLE_SCHEMA = '{schema}'
  """
  
  sf_cursor.execute(query)
  views = sf_cursor.fetchall()
  
  views = [view[0] for view in views]
  
  st.session_state['views'] = views
  # st.session_state.df['view_name'] = views

def fetch_schemas(sf_cursor):
  query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"
  
  sf_cursor.execute(query)
  schemas = sf_cursor.fetchall()
  schemas = [schema[0] for schema in schemas]
  
  st.session_state["schemas"] = schemas

# @st.cache_data
def execute_views_and_get_row_counts():
  schema = st.session_state['selected_schema']
  sf_cursor = st.session_state['sf_cursor']
  views = st.session_state['selected_views']
      
  # st.session_state.df.loc[len(st.session_state.df)] = [view, 0, 'Pending', '']
  for idx, view in enumerate(views):
    with st.spinner(f"Processing {view}"):
      try:
          query = f"SELECT COUNT(*) FROM {schema.upper()}.{view.upper()}"
          sf_cursor.execute(query)
          row_count = sf_cursor.fetchone()
          logging.info(f"SUCCESS: Executed view '{view}' with row count: {row_count}")
          st.session_state.df.loc[st.session_state.df['view_name'] == view, ['row_count', 'status', 'error']] = [row_count, 'SUCCESS', '']
      except snowflake.connector.errors.ProgrammingError as e:
          error_message = str(e)
          logging.error(f"ERROR: Failed to execute view '{view}'. Error: {error_message}")
          st.session_state.df.loc[st.session_state.df['view_name'] == view, ['row_count', 'status', 'error']] = [None, 'ERROR', error_message]  
      except Exception as ex:
          error_message = str(ex)
          logging.error(f"ERROR: Unexpected error while executing view '{view}'. Error: {error_message}")
          st.session_state.df.loc[st.session_state.df['view_name'] == view, ['row_count', 'status', 'error']] = [None, 'ERROR', error_message] 
         
  st.experimental_rerun()
      
with st.sidebar:
  st.header("Settings")
  
  with st.expander("SF Connection Details"):
    user = st.text_input("Email", value="")
    
    account = st.text_input("Account", disabled=True, value="htb40669.us-east-1")
    role = st.text_input("Role", disabled=True, value="DP_IBS_MANA_NTW_ETL_QA")
    warehouse = st.text_input("Warehouse", disabled=True, value="WH_NTW_QA")
    database = st.text_input("DB", disabled=True, value="IBS_MANA_QA")
    
    col1, col2 = st.columns(2)
    with col1:
      if st.button("Connect"):
        if user:
          connect_to_snowflake(user)
    # with col2:
    #   if st.button("Disconnect"):
    #     with st.spinner("Disconnecting from Snowflake"):
    #       st.session_state['sf_cursor'].close()
    #       st.session_state['sf_conn'].close()  

    #       st.success("Disconnected from Snowflake!")  
                      
  if len(st.session_state['schemas']) > 0:
    selected_schema = st.selectbox("Select Schema:", st.session_state['schemas'])
    st.session_state['selected_schema'] = selected_schema
    
    with st.form(key="my_form"):
      with st.spinner("Fetching Views"):
        fetch_views(selected_schema, st.session_state['sf_cursor'])
      
      # if len(st.session_state['views']) > 0:
      selected_views = st.multiselect(
        'Select Views:',
        options=st.session_state['views'],
        default=None,
        help='You can search and select multiple views'
      )
      submit_btn = st.form_submit_button(label='Submit')
    
    if submit_btn:
      st.session_state['selected_views'] = selected_views
      st.session_state.df['view_name'] = selected_views
      st.session_state.df['status'] = "PENDING"
      st.experimental_rerun()

cell_style_jscode = JsCode("""
function(params) {
  if (params.value == "SUCCESS") {
    return {
      'backgroundColor': 'green',
      'color': 'white'
    };
  } else if (params.value == "ERROR") {
    return {
      'backgroundColor': 'red',
      'color': 'white'
    };
  } else {
    return {
      'backgroundColor': 'orange',
      'color': 'white'
    };
  }
};                         
""")    
  
gb = GridOptionsBuilder.from_dataframe(st.session_state['df'])  
gb.configure_default_column(editable=False, filter=True, resizable=True, sortable=True, groupable=True)  
gb.configure_selection(selection_mode="multiple", use_checkbox=True)
# gb.configure_pagination(paginationAutoPageSize=True, paginationPageSize=30)
gb.configure_side_bar()
gb.configure_grid_options(suppressMovableColumns=True, enable_pivot=True)
gb.configure_column(
    'view_name',
    cellStyle={"backgroundColor": "#7a7fa1", "color": "white"},
)
gb.configure_column('status', cellStyle=cell_style_jscode)

grid_options = gb.build()
      
st.subheader("Data Grid:")

grid_response = AgGrid(
  st.session_state['df'],
  theme='dark',
  gridOptions=grid_options,
  enable_enterprise_modules=True,
  update_mode=GridUpdateMode.VALUE_CHANGED,
  data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
  fit_columns_on_grid_load=True,
  allow_unsafe_jscode=True,
  height=600,
)     

btn_disabled = st.session_state['selected_views'] == []
validate_btn = st.button('Run Validation', disabled=btn_disabled)
    
if validate_btn:  
  st.session_state.df['view_name'] = st.session_state['selected_views']
  # st.session_state.df['status'] = 'PENDING'
  execute_views_and_get_row_counts()  
       