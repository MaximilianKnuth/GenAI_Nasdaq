import os
from transformers import pipeline

class TaskClassificationAgent:
    def __init__(self, model_name="facebook/bart-large-mnli"):
        """
        Initializes the Task Classification Agent with a zero-shot classification pipeline.
        """
        self.classifier = pipeline("zero-shot-classification", model=model_name)
        # The possible tasks/labels we want to classify into:
        self.labels = ["convert_datetime", "join_tables", "check_distribution"]
    
    def classify(self, user_query):
        """
        Uses a zero-shot classifier to identify the most likely task 
        among our predefined set of labels.
        """
        # Perform zero-shot classification
        result = self.classifier(user_query, self.labels)
        
        # The pipeline returns scores for each label. The top label is the predicted task.
        predicted_task = result["labels"][0]  # label with the highest score

        # Return a dictionary consistent with our previous usage
        return predicted_task
    
    
    
    
    """def classify(self, user_query):
        query_lower = user_query.lower()
        
        if "convert datetime" in query_lower or "utc" in query_lower:
            return {"task": "convert_datetime", "columns": ["date", "updated"]}
        
        elif "join" in query_lower and "tables" in query_lower:
            return {"task": "join_tables", "tables": ["EFR", "EQR"], "join_on": "ticker"}
        
        elif "skew" in query_lower or "distribution" in query_lower:
            return {"task": "check_distribution", "columns": ["eqr", "sgr"]}
        
        return None"""