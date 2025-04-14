import os
import openai
import traceback
import pandas as pd
from typing import List, Union

class CodeGenerationAgent:
    def __init__(self, api_key, base_url="https://api.deepseek.com", model="deepseek-coder", max_retries=3):
        self.api_key = api_key
        self.model = model
        self.client = openai.OpenAI(api_key=api_key, base_url=base_url)
        self.max_retries = max_retries

    def _clean_code_block(self, code: str) -> str:
        """Remove Markdown code block markers if present"""
        if code.startswith('```python') and code.endswith('```'):
            return code[9:-3].strip()
        elif code.startswith('```') and code.endswith('```'):
            return code[3:-3].strip()
        return code

    def _read_csv_head(self, csv_path: str, n_rows: int = 5) -> str:
        """Read first n rows of CSV and return as string"""
        try:
            df = pd.read_csv(csv_path, nrows=n_rows)
            return df.to_markdown(index=False)
        except Exception as e:
            return f"[Error reading {csv_path}: {str(e)}]"

    def generate_code_prompt(self, user_query: str, execution_summary: str, csv_paths: Union[str, List[str]]) -> str:
        """Generate prompt with either:
        - Single dataset head (if 1 path)
        - Multiple dataset heads (if multiple paths)"""
        
        # Handle single path vs list
        paths = [csv_paths] if isinstance(csv_paths, str) else csv_paths
        
        # Build dataset info section
        if len(paths) == 1:
            dataset_info = f"""
            ### Dataset Preview (first 5 rows):
            {self._read_csv_head(paths[0])}
            """
        else:
            dataset_info = "\n### Multiple Datasets Preview:\n"
            for path in paths:
                dataset_info += f"\n**{os.path.basename(path)}**:\n{self._read_csv_head(path)}\n"
        
        return f"""
        You are a Python code generation assistant. The user provided:

        QUERY: "{user_query}"

        EXECUTION CONTEXT:
        {execution_summary}

        {dataset_info}

        TASK REQUIREMENTS:
        1. Process ALL datasets listed above
        2. Perform data transformation as specified
        3. Start with loading required dataset from a sample path
        4. Include code to save transformed data with '_transformed' suffix
        5. Return ONLY the executable Python code without any Markdown formatting
        """

    def request_code_from_model(self, prompt: str) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a helpful Python coding assistant."},
                {"role": "user", "content": prompt},
            ],
            temperature=0
        )
        return response.choices[0].message.content.strip()

    def write_code_to_file(self, code: str, filename: str = "generated_code.py"):
        """Write code to file after cleaning Markdown blocks"""
        cleaned_code = self._clean_code_block(code)
        print(cleaned_code)
        with open(filename, "w") as f:
            f.write(cleaned_code)

    def execute_generated_code(self, filename: str = "generated_code.py"):
        """Execute generated code and capture output"""
        local_vars = {}
        with open(filename, "r") as f:
            code = f.read()
        exec(code, globals(), local_vars)
        return local_vars.get('output_files', [])

    def run(self, user_query: str, execution_summary: str, csv_path: Union[str, List[str]]):
        prompt = self.generate_code_prompt(user_query, execution_summary, csv_path)

        for attempt in range(1, self.max_retries + 1):
            print(f"\n[Attempt {attempt}/{self.max_retries}] Processing {len(csv_path) if isinstance(csv_path, list) else 1} datasets...")
            try:
                code = self.request_code_from_model(prompt)
                self.write_code_to_file(code)
                output_files = self.execute_generated_code()
                
                if output_files:
                    print("\n✅ Successfully created:")
                    for file in output_files:
                        print(f"- {file}")
                else:
                    print("\n✅ Code Test Done")
                return True
            except Exception as e:
                print(f"\n❌ Attempt {attempt} failed: {str(e)}")
                traceback.print_exc()
                if attempt == self.max_retries:
                    print("\n🛑 Max retries reached")
                    return False