from Agents.data_transformation_agent import DataTransformationAgent
from Agents.task_classification_agent import TaskClassificationAgent
from Agents.table_join_agent import TableJoinAgent
from Agents.data_validation_agent import DataValidationAgent
import json


class MasterAgent:
    def __init__(self):
        self.task_agent = TaskClassificationAgent()
        self.execution_agents = {
            "convert_datetime": DataTransformationAgent(),
            "join_tables": TableJoinAgent(),
            "check_distribution": DataValidationAgent()
        }

    def process_query(self, user_query, df_dict):
        """
        Processes user query, classifies the task, and delegates execution.
        Returns a structured pipeline log.
        """
        action = self.task_agent.classify(user_query)
        
        if action:
            sub_agent = self.execution_agents.get(action)

            if sub_agent:
                print(f"Executing Task: {action}")
                return sub_agent.execute(user_query,df_dict)
            else:
                print(f"No agent found for task: {action}")
        else:
            print("Could not classify the task.")

        return None