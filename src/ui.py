import streamlit as st
import requests
import time

# --- Configuration ---
API_BASE_URL = "http://localhost:8000/api/v1"
PAGE_CONFIG = {
    "page_title": "COBOL Code Analyzer",
    "page_icon": "🤖",
    "layout": "wide",
}

st.set_page_config(**PAGE_CONFIG)

# --- Helper Functions ---

@st.cache_data(ttl=600)  # Cache reports for 10 minutes
def get_report(report_name: str):
    """Fetches a markdown report from the API."""
    try:
        url = f"{API_BASE_URL}/reports/{report_name}"
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        st.error(f"Failed to fetch {report_name} report: {e}")
        return None

def show_report_page(title: str, report_name: str, help_text: str):
    """Renders a report page."""
    st.header(title)
    st.info(help_text)
    
    with st.spinner(f"Loading {title}..."):
        report_md = get_report(report_name)
    
    if report_md:
        # if report_name == "call-graphs":
        #     # Streamlit Markdown natively supports Mermaid!
        #     st.markdown(report_md)
        # else:
        #     # Use unsafe_allow_html for better table/list rendering
        st.markdown(report_md, unsafe_allow_html=True)

def show_chat_page():
    """Renders the main chat interface."""
    st.header("Chat with your COBOL Knowledge Base")
    st.info("Ask questions about your code, user stories, or call flows.")

    # Initialize chat history in session state
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display past messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Handle new user input
    if prompt := st.chat_input("What would you like to know?"):
        # Add user message to history
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Get assistant response
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response = ""
            
            try:
                # Post to the streaming API endpoint
                with requests.post(
                    f"{API_BASE_URL}/chat/query",
                    json={"query": prompt},
                    stream=True
                ) as r:
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=None, decode_unicode=True):
                        full_response += chunk
                        message_placeholder.markdown(full_response + "▌")
                
                message_placeholder.markdown(full_response)
                
            except requests.RequestException as e:
                full_response = f"Error connecting to API: {e}"
                message_placeholder.error(full_response)
        
        # Add assistant response to history
        st.session_state.messages.append({"role": "assistant", "content": full_response})

# --- Main App Navigation ---

st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    ["Chat", "Code Summaries", "Call Graphs", "User Stories"]
)

if page == "Chat":
    show_chat_page()

elif page == "Code Summaries":
    show_report_page(
        "Code Summaries", 
        "summaries",
        "A hierarchical view of all summarized programs, divisions, sections, and paragraphs."
    )

elif page == "Call Graphs":
    show_report_page(
        "Program Call Graphs", 
        "call-graphs",
        "The complete call graph for each program's Procedure Division, rendered in Mermaid."
    )

elif page == "User Stories":
    show_report_page(
        "Generated User Stories", 
        "user-stories",
        "Business-focused user stories generated from the technical code summaries."
    )