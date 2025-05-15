# langgraph_implementation.py  – build the workflow
from langgraph.graph import StateGraph
from state_schema import AgentState
from agent_functions import (
    task_classification_agent,
    split_query_agent,
    tz_rag_agent,
    join_table_rag_agent,
    timezone_extraction_agent,
    existence_and_column_type_check,
    join_table_check,
    # validation_agent,
    code_generation_agent,
    process_human_input,
    llm_first_pass
)



def task_router(state: AgentState) -> dict:
    #if state.task_step==1 and state.first_run:
        #print("🟢 route to specific task agent")
    #elif state.task_step >1:
        #print("🟢 route to subsequent task agent")

    # Standard routing logic (first pass)
    task=state.task_list[state.task_step-1]
    if task=="convert_datetime":
        decision = "execute_timezone_subagent"
    elif task=="join_tables":
        decision = "execute_join_table_subagent"
    else:
        decision = "done"
    return {"routing_decision": decision}

def router_node_timezone(state: AgentState) -> dict:
    #if state.first_run and state.first_run:
        #print("🟢 tz_router_node")

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
            decision = "existence_and_column_type_check"    
        elif state.task_step<state.task_length:
            decision="subsequent_task_execute"
            state.task_step=state.task_step+1
            state.first_run=True
            state.completeness_check_result=False
        else: #Go to validation
            decision = "done"
            
        return {"routing_decision": decision,"task_step": state.task_step,"first_run": state.first_run,"completeness_check_result":state.completeness_check_result}
    
    # Standard routing logic (first pass)
    if not (state.table_name and state.datetime_columns):
        decision = "need_table"
    elif not (state.original_timezone and state.target_timezone):
        decision = "need_tz"
    elif state.completeness_check_result is False:
        decision = "existence_and_column_type_check"    
    elif state.task_step<state.task_length:
        decision="subsequent_task_execute"
        state.task_step=state.task_step+1
        state.first_run=True
        state.completeness_check_result=False
    else:
        decision = "done"
    
    return {"routing_decision": decision,"task_step": state.task_step,"first_run": state.first_run,"completeness_check_result":state.completeness_check_result}

def router_node_join_table(state: AgentState) -> dict:
    #if state.first_run and state.first_run:
        #print("🟢 jt_router_node")

    """
    Enhanced router that makes intelligent decisions after human input
    """
    # First check if human input is needed
    if state.needs_human_input:
        return {"routing_decision": "ask_human"}
    
    # After human input was processed, route based on what's still missing
    if state.human_response:
        # Check what's missing and route to appropriate agent
        if not (state.table_name1 and state.join_column1 and state.table_name2 and state.join_column2):
            print(state.table_name1 and state.join_column1 and state.table_name2 and state.join_column2)
            print(state.table_name1)
            print(state.join_column1)
            print(state.table_name2)
            print(state.join_column2)
            decision = "need_table"  # Go to RAG agent with human input context
        elif state.completeness_check_result is False:
            decision = "join_table_check"    
        elif state.task_step<state.task_length:
            decision="subsequent_task_execute"
            state.task_step=state.task_step+1
            state.first_run=True
            state.completeness_check_result=False
        else: #Go to validation
            decision = "done"
            
        return {"routing_decision": decision,"task_step": state.task_step,"first_run": state.first_run,"completeness_check_result":state.completeness_check_result}
    
    
    # Standard routing logic (first pass)
    if not (state.table_name1 and state.join_column1 and state.table_name2 and state.join_column2):
        decision = "need_table"
    elif state.completeness_check_result is False:
        decision = "join_table_check"    
    elif state.task_step<state.task_length:
        decision="subsequent_task_execute"
        state.task_step=state.task_step+1
        state.first_run=True
        state.completeness_check_result=False
    else:
        decision = "done"
    
    return {"routing_decision": decision,"task_step": state.task_step,"first_run": state.first_run,"completeness_check_result":state.completeness_check_result}


    # Add a new routing function that simply returns the stored decision

def route_by_decision(state: AgentState) -> str:
    return state.routing_decision

# Define a function to route based on whether this is the first run
def first_run_router(state: AgentState) -> dict:
    #print("first_run_router")
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
    g.add_node("split_query_agent",split_query_agent)
    g.add_node("task_router", task_router)
    g.add_node("router_timezone", router_node_timezone)
    g.add_node("router_join_table", router_node_join_table)
    g.add_node("tz_rag_lookup", tz_rag_agent)
    g.add_node("join_table_rag_lookup",join_table_rag_agent)
    g.add_node("tz_extractor", timezone_extraction_agent)
    g.add_node("existence_and_column_type_check", existence_and_column_type_check)
    g.add_node("join_table_check", join_table_check)
    g.add_node("human_input", process_human_input)
    # g.add_node("validation", validation_agent)
    g.add_node("code_generation", code_generation_agent)

    

    # Then define the conditional edges using the function's return values
    # With this:
    g.set_entry_point("first_run_router")
    
    # — normal edges —
    g.add_edge("split_query_agent","llm_first_pass")
    g.add_edge("llm_first_pass","task_router")
    g.add_edge("human_input", "task_router") 
    # g.add_edge("validation", "code_generation")
    
    
    # - tz extraction edges - 
    g.add_edge("tz_rag_lookup", "existence_and_column_type_check")
    g.add_edge("tz_extractor", "existence_and_column_type_check")
    g.add_edge("existence_and_column_type_check", "router_timezone")

    # - jt extraction edges - 
    g.add_edge("join_table_rag_lookup", "join_table_check")
    g.add_edge("join_table_check", "router_join_table")
    
    
    g.add_conditional_edges(
        "task_classification",
        lambda state: "write separate queries" if len(state.task_list) > 1 else "pass",
        {
            "write separate queries": "split_query_agent",
            "pass": "llm_first_pass"
        }
    )
    
    g.add_conditional_edges(
    "first_run_router",
    lambda state: state.routing_decision,  # Use the routing_decision field
    {
        "first_run": "task_classification",
        "human_input_needed": "human_input",
        "subsequent_run": "task_router"
    }
    )   

    # task-Router conditional edges
    g.add_conditional_edges(
    "task_router",
    lambda state: state.routing_decision,  # Use the routing_decision field
    {
        "execute_timezone_subagent": "router_timezone",
        "execute_join_table_subagent": "router_join_table"
    }
    ) 
    
    # timezone-Router conditional edges
    g.add_conditional_edges(
        "router_timezone",
        route_by_decision,
        {
            "need_table": "tz_rag_lookup",
            "need_tz": "tz_extractor",
            "ask_human": "human_input",
            "existence_and_column_type_check": "existence_and_column_type_check",
            "subsequent_task_execute": "llm_first_pass",
            "done": "code_generation",
        },
    )

    # jointable-Router conditional edges
    g.add_conditional_edges(
        "router_join_table",
        route_by_decision,
        {
            "need_table": "join_table_rag_lookup",
            "ask_human": "human_input",
            "join_table_check": "join_table_check",
            "subsequent_task_execute": "llm_first_pass",
            "done": "code_generation",
        },
    )
    
    # — finish —
    g.set_finish_point("code_generation")

    return g.compile()