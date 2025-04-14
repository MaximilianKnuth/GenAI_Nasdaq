from Agents.data_transformation_agent import DataTransformationAgent
from Agents.task_classification_agent import TaskClassificationAgent
from Agents.table_join_agent import TableJoinAgent
from Agents.data_validation_agent import DataValidator
from Agents.code_generation_agent import CodeGenerationAgent
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
        #print(f"Executing {sub_agent.name}")
        summary = sub_agent.execute(self.user_query, self.df_dict, self.api_key)
        if summary:
            return summary
        else:
            print(f"Executing {sub_agent.name} failed")
    
    def execute_task(self):
            action = self.classify_task()
            sub_agent = self.get_sub_agent(action)
            summary = self.execute_sub_agent(sub_agent)
            print("summary")
            print(summary)
            self._route_to_code_generation(self.user_query,summary)
            return summary
        
    def _route_to_code_generation(self, user_query, summary):
        """
        Routes the query to the code generation agent.
        """
        print("Routing to CodeGenerationAgent...")
        
        def extract_executed_tables(summary_text):
            """
            Extracts the table name(s) from the conversion summary text.
            
            Args:
                summary_text (dict): Dictionary containing "Executed Table" key.
                    Example: {"Executed Table": "EQR"} 
                            or {"Executed Table": "EQR,ABC,XYZ"}
                
            Returns:
                list: A list of table names (even if only one).
            """
            # Get the raw table string (e.g., "EQR" or "EQR,ABC,XYZ")
            tables_str = summary_text["Executed Table"].strip()
            
            if not tables_str:
                print("Oops, there's no CSV file to execute on...")
                return []
            
            # Split ONLY if comma exists, otherwise treat as single table
            if "," in tables_str:
                tables = [table.strip() for table in tables_str.split(",")]
            else:
                tables = [tables_str]  # Keep as single-item list
            
            # Convert to file paths
            file_paths = [f"01_Data/{table}.csv" for table in tables]

            return file_paths


        file_paths = extract_executed_tables(summary)

        agent = CodeGenerationAgent(api_key=self.api_key)
        agent.run(user_query=user_query, execution_summary=summary, csv_path=file_paths)

    
            
    
    
            
        

    