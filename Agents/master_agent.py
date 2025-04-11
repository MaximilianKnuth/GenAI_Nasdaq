from Agents.data_transformation_agent import DataTransformationAgent
from Agents.task_classification_agent import TaskClassificationAgent
from Agents.table_join_agent import TableJoinAgent
from Agents.data_validation_agent import DataValidator
import json

class MasterAgent:
    def __init__(self,user_query, df_dict,api_key):
        """
        Initializes the Execution Agent.
        """
        self.user_query = user_query
        self.df_dict = df_dict
        self.api_key = api_key
        self.data_validator = DataValidator()
        self.task_classifier = TaskClassificationAgent()
        self.execution_agents = {
            "convert_datetime": DataTransformationAgent(data_validator=self.data_validator),  
            #"join_tables": TableJoinAgent()
        }
    
    def classify_task(self):
        action = self.task_classifier.classify(self.user_query)
        if action:
            return action
        else:
            print("Could not classify the task.")
            return None
    
    def get_sub_agent(self, action):
        sub_agent = self.execution_agents.get(action)
        if sub_agent:
            return sub_agent
        else:
            print(f"No agent found for task: {action}")
            return None
    
    def execute_sub_agent(self, sub_agent):
        print(f"Executing {sub_agent.name}")
        summary = sub_agent.execute(self.user_query, self.df_dict, self.api_key)
        if summary:
            return summary
        else:
            print(f"Executing {sub_agent.name} failed")
    
    def execute_task(self):
        action = self.classify_task()
        sub_agent = self.get_sub_agent(action)
        summary = self.execute_sub_agent(sub_agent)
        print(summary)
        return summary
        
              
        
        
            
        

    