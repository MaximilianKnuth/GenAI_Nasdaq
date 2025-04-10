from Agents.execution_agent import ExecutionAgent
import json


class MasterAgent:
    def __init__(self):
        """
        Initializes the Master Agent with optional schema context.
        """
        self.execution_agent = None

    def process_query(self, user_query, df_dict,api_key):
        self.execution_agent = ExecutionAgent(user_query, df_dict,api_key)
        self.execution_agent.execute_query()