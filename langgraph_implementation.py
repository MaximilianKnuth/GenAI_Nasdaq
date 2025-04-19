# langgraph_implementation.py  – build the workflow
from langgraph.graph import StateGraph
from state_schema import AgentState
from agent_functions import (
    task_classification_agent,
    rag_agent,
    timezone_extraction_agent,
    completeness_check,
    validation_agent,
    code_generation_agent,
    process_human_input,
    llm_first_pass
)
def router_node(state: AgentState) -> dict:
    print("router_node")
    """
    Enhanced router that makes intelligent decisions after human input
    """
    # First check if human input is needed
    if state.needs_human_input:
        return {"routing_decision": "ask_human"}
    
    # After human input was processed, route based on what's still missing
    if state.human_response:
        # Check what's missing and route to appropriate agent
        if not (state.table_name and state.datetime_columns):
            decision = "need_table"  # Go to RAG agent with human input context
        elif not (state.original_timezone and state.target_timezone):
            decision = "need_tz"     # Go to timezone extractor with human input context
        elif state.completeness_check_result is False:
            decision = "completeness_check"    
        else:
            decision = "done"
            
        return {"routing_decision": decision}
    
    # Standard routing logic (first pass)
    if not (state.table_name and state.datetime_columns):
        decision = "need_table"
    elif not (state.original_timezone and state.target_timezone):
        decision = "need_tz"
    elif state.completeness_check_result is False:
        decision = "completeness_check"    
    else:
        decision = "done"
    
    return {"routing_decision": decision}

    # Add a new routing function that simply returns the stored decision
def route_by_decision(state: AgentState) -> str:
    return state.routing_decision

# Define a function to route based on whether this is the first run
def first_run_router(state: AgentState) -> dict:
    print("first_run_router")
    """Router that determines the initial processing path"""
    if state.first_run:
        return {"routing_decision": "first_run"}
    elif state.human_response:
        return {"routing_decision": "human_input_needed"}
    else:
        return {"routing_decision": "subsequent_run"}
        
def create_agent_workflow():
    g = StateGraph(AgentState)

    # — nodes —
    g.add_node("first_run_router", first_run_router)
    g.add_node("llm_first_pass", llm_first_pass)
    g.add_node("task_classification", task_classification_agent)
    g.add_node("router", router_node)
    g.add_node("rag_lookup", rag_agent)
    g.add_node("tz_extractor", timezone_extraction_agent)
    g.add_node("completeness_check", completeness_check)
    g.add_node("human_input", process_human_input)
    g.add_node("validation", validation_agent)
    g.add_node("code_generation", code_generation_agent)

    

    # Then define the conditional edges using the function's return values
    # With this:
    g.set_entry_point("first_run_router")
    
    # — normal edges —
    g.add_edge("task_classification", "llm_first_pass")
    g.add_edge("llm_first_pass", "router")
    g.add_edge("human_input", "router") 
    g.add_edge("rag_lookup", "completeness_check")
    g.add_edge("tz_extractor", "completeness_check")
    g.add_edge("completeness_check", "router")
    g.add_edge("validation", "code_generation")
    
    g.add_conditional_edges(
    "first_run_router",
    lambda state: state.routing_decision,  # Use the routing_decision field
    {
        "first_run": "task_classification",
        "human_input_needed": "human_input",
        "subsequent_run": "router"
    }
)   

    # Router conditional edges
    g.add_conditional_edges(
        "router",
        route_by_decision,
        {
            "need_table": "rag_lookup",
            "need_tz": "tz_extractor",
            "ask_human": "human_input",
            "completeness_check": "completeness_check",
            "done": "validation",
        },
    )

    # — finish —
    g.set_finish_point("code_generation")

    return g.compile()