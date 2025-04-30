import streamlit as st
import pandas as pd
from main import DataProcessingApp
import os
import subprocess
import sys
from io import StringIO
import contextlib

# Set up the app
st.set_page_config(page_title="Data Processing Assistant", layout="wide")

# Custom CSS for better styling
st.markdown("""
<style>
    .stTextInput input {
        font-size: 18px;
    }
    .stButton button {
        width: 100%;
        padding: 10px;
        font-size: 16px;
    }
    .stMarkdown h1, h2, h3 {
        color: #2c3e50;
    }
    .info-box {
        background-color: #f8f9fa;
        border-radius: 5px;
        padding: 15px;
        margin-bottom: 20px;
        border-left: 4px solid #3498db;
    }
    .code-box {
        background-color: #f5f5f5;
        border-radius: 5px;
        padding: 15px;
        font-family: monospace;
        white-space: pre-wrap;
        overflow-x: auto;
    }
    .success-box {
        background-color: #e8f5e9;
        border-radius: 5px;
        padding: 15px;
        margin-bottom: 20px;
        border-left: 4px solid #4caf50;
    }
    .error-box {
        background-color: #ffebee;
        border-radius: 5px;
        padding: 15px;
        margin-bottom: 20px;
        border-left: 4px solid #f44336;
    }
    .question-box {
        background-color: #e3f2fd;
        border-radius: 5px;
        padding: 15px;
        margin-bottom: 20px;
        border-left: 4px solid #2196f3;
    }
</style>
""", unsafe_allow_html=True)

# Initialize the app
@st.cache_resource
def get_processing_app():
    return DataProcessingApp()

app = get_processing_app()

# Session state to track the workflow state
if 'workflow_state' not in st.session_state:
    st.session_state.workflow_state = None
if 'user_query' not in st.session_state:
    st.session_state.user_query = ""
if 'human_response' not in st.session_state:
    st.session_state.human_response = ""
if 'processing_messages' not in st.session_state:
    st.session_state.processing_messages = []
if 'generated_code' not in st.session_state:
    st.session_state.generated_code = None
if 'code_output' not in st.session_state:
    st.session_state.code_output = None

# Main app
st.title("📊 Data Processing Assistant")
st.markdown("""
<div class="info-box">
    This assistant helps with data processing tasks like timezone conversion and table joins. 
    Enter your query below and the system will guide you through the process.
</div>
""", unsafe_allow_html=True)

# User input section
with st.form("query_form"):
    st.session_state.user_query = st.text_input(
        "Enter your data processing query:", 
        value=st.session_state.user_query,
        placeholder="e.g., 'Convert timestamps in EQR from EST to UTC then join with EFR on ticker'"
    )
    submit_button = st.form_submit_button("Process Query")

# Process the query
if submit_button and st.session_state.user_query:
    st.session_state.processing_messages = []
    st.session_state.generated_code = None
    st.session_state.code_output = None
    
    # Initialize state
    state = {
        "user_query": st.session_state.user_query,
        "api_key": app.api_key,
        "openai_api_key": app.openai_api_key,
        "df_dict": app.df_dict,
        "first_run": True,
        "human_response": None,
        "needs_human_input": False
    }
    
    # Start the workflow
    event_stream = app.workflow.stream(state, stream_mode="debug")
    
    # Process events
    for ev in event_stream:
        if ev["type"] == "task":
            task_name = ev["payload"]["name"]
            st.session_state.processing_messages.append(f"▶️ Processing: {task_name}")
            
        elif ev["type"] == "task_result":
            node = ev["payload"]["name"]
            result = ev["payload"]["result"]
            
            if isinstance(result, dict):
                if "needs_human_input" in result and result["needs_human_input"]:
                    st.session_state.workflow_state = result
                    st.session_state.processing_messages.append(f"❓ Question: {result['human_message']}")
                    break
                
                if "generated_code" in result:
                    st.session_state.generated_code = result["generated_code"]
                    st.session_state.processing_messages.append("✅ Code generated successfully!")
            
            st.session_state.processing_messages.append(f"✓ Completed: {node}")
            
        elif ev["type"] == "workflow_end":
            final_state = ev["payload"]["state"]
            if "generated_code" in final_state:
                st.session_state.generated_code = final_state["generated_code"]
                st.session_state.processing_messages.append("✅ Workflow completed successfully!")
            else:
                st.session_state.processing_messages.append("⚠️ Workflow completed but no code was generated")

# Display processing messages
if st.session_state.processing_messages:
    st.subheader("Processing Log")
    with st.expander("Show processing details", expanded=True):
        for msg in st.session_state.processing_messages:
            if msg.startswith("❓"):
                st.markdown(f'<div class="question-box">{msg}</div>', unsafe_allow_html=True)
            elif msg.startswith("⚠️"):
                st.markdown(f'<div class="error-box">{msg}</div>', unsafe_allow_html=True)
            elif msg.startswith("✅"):
                st.markdown(f'<div class="success-box">{msg}</div>', unsafe_allow_html=True)
            else:
                st.info(msg)

# Handle human input if needed
if st.session_state.workflow_state and st.session_state.workflow_state.get("needs_human_input"):
    with st.form("human_input_form"):
        st.markdown(f'<div class="question-box">{st.session_state.workflow_state["human_message"]}</div>', unsafe_allow_html=True)
        human_response = st.text_input("Your response:", key="human_response_input")
        submit_human_response = st.form_submit_button("Submit")
        
        if submit_human_response and human_response:
            # Update state with human response and continue processing
            new_state = {
                **st.session_state.workflow_state,
                "human_response": human_response,
                "needs_human_input": False,
                "first_run": False
            }
            
            # Restart the workflow with updated state
            event_stream = app.workflow.stream(new_state, stream_mode="debug")
            
            # Clear the human input state
            st.session_state.workflow_state = None
            st.session_state.processing_messages.append(f"📝 User provided input: {human_response}")
            
            # Rerun to show updated messages
            st.rerun()

# Display generated code
if st.session_state.generated_code:
    st.subheader("Generated Code")
    with st.expander("View generated code", expanded=True):
        st.code(st.session_state.generated_code, language='python')
    
    # Button to run the code
    if st.button("▶️ Run Generated Code"):
        # Save code to a temporary file
        with open("temp_generated_code.py", "w") as f:
            f.write(st.session_state.generated_code)
        
        # Capture output
        output = StringIO()
        with contextlib.redirect_stdout(output):
            try:
                subprocess.run([sys.executable, "temp_generated_code.py"], check=True)
                st.session_state.code_output = "✅ Code executed successfully!\n" + output.getvalue()
            except subprocess.CalledProcessError as e:
                st.session_state.code_output = f"❌ Error executing code:\n{str(e)}\n\nOutput:\n{output.getvalue()}"
        
        # Rerun to show output
        st.rerun()

# Display code execution output
if st.session_state.code_output:
    st.subheader("Execution Output")
    if st.session_state.code_output.startswith("✅"):
        st.markdown(f'<div class="success-box">{st.session_state.code_output}</div>', unsafe_allow_html=True)
    else:
        st.markdown(f'<div class="error-box">{st.session_state.code_output}</div>', unsafe_allow_html=True)
    
    # Show output files if they exist
    output_files = []
    for fname in ["output.csv", "joined_output.csv"]:
        if os.path.exists(fname):
            output_files.append(fname)
    
    if output_files:
        st.markdown("### Generated Output Files")
        for fname in output_files:
            st.download_button(
                label=f"Download {fname}",
                data=open(fname, "rb").read(),
                file_name=fname,
                mime="text/csv"
            )
            
            # Show preview
            try:
                df = pd.read_csv(fname)
                st.dataframe(df.head())
            except Exception as e:
                st.error(f"Could not preview {fname}: {str(e)}")

# Example queries
st.sidebar.markdown("### Example Queries")
example_queries = [
    "Convert the date column in the EQR dataset from EST to UTC",
    "Join EFR and EQR based on ticker",
    "Convert New_date in EQR from EST to UTC then join with EFR on ticker"
]

for query in example_queries:
    if st.sidebar.button(query):
        st.session_state.user_query = query
        st.rerun()