import os
import re
import sys
from typing import List, Dict, Any

# langchain-related imports
from langchain_community.vectorstores import Chroma
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.chains import RetrievalQA
from langchain_community.docstore.document import Document
from langchain_openai import OpenAIEmbeddings
from langchain_openai import ChatOpenAI

# NEW: import pdfplumber
import pdfplumber
# Suppress PyMuPDF warnings globally
sys.stderr = open(os.devnull, 'w')

class RAG_retrieval:
    def __init__(self, pdf_folder: str, openai_api_key: str, deepseek_api_key: str):
        """
        Initialize the RAG class with required folder path, OpenAI API key, and DeepSeek API key.
        """
        self.pdf_folder = pdf_folder
        self.openai_api_key = openai_api_key
        self.deepseek_api_key = deepseek_api_key

        # Text splitter for chunking text
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            separators=["\n\n", "\n", ". ", " ", ""]
        )

        # Will be populated after creating the vector store
        self.vectorstore = None

        # Will be populated after creating the QA chain
        self.qa_chain = None

    # 1. Detect content type
    def detect_content_type(self, text: str) -> str:
        lower_text = text.lower()
        if "create table" in lower_text or "schema:" in lower_text:
            return "schema"
        elif re.search(r"\b(def|class|function)\b", lower_text):
            return "code"
        elif "definition:" in lower_text or "term:" in lower_text:
            return "definition"
        else:
            return "general"

    # 2. PDF parser using pdfplumber
    def extract_text_with_pdfplumber(self, pdf_path: str) -> List[Dict[str, Any]]:
        """
        Extract text from PDF using pdfplumber.
        Returns a list of dictionaries with "content", "page_num", and "source".
        """
        #print(f"Extracting text from {pdf_path} using pdfplumber...")
        pages_content = []

        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text()
                # Some PDFs might return None if the page is empty or unreadable
                if text:
                    # Clean the text
                    text = re.sub(r'\s+', ' ', text)
                    text = text.strip()

                    if text:  # Only add non-empty text
                        pages_content.append({
                            "content": text,
                            "page_num": page_num,
                            "source": os.path.basename(pdf_path)
                        })
        return pages_content

    # 3. Load PDFs with improved parsing and apply semantic chunking
    def load_and_chunk_pdfs(self) -> List[Dict[str, Any]]:
        """
        Load all PDFs from the given folder, extract text using pdfplumber,
        detect content type, and split into chunks as appropriate.
        """
        #print(f"Loading PDFs from folder: {self.pdf_folder}")
        all_chunks = []
        
        for filename in os.listdir(self.pdf_folder):
            if filename.endswith(".pdf"):
                pdf_path = os.path.join(self.pdf_folder, filename)
                #print(f"Processing file: {filename}")
                
                try:
                    pages_content = self.extract_text_with_pdfplumber(pdf_path)
                except Exception as e:
                    print(f"pdfplumber extraction failed for {filename}: {e}")
                    continue  # Skip this file if extraction fails

                #print(f"Extracted {len(pages_content)} pages from {filename}")
                
                for page in pages_content:
                    content_type = self.detect_content_type(page["content"])
                    
                    # Apply different chunking based on content type
                    if content_type in ["schema", "code", "definition"]:
                        #print(f"Detected content type as {content_type}. Not splitting further.")
                        all_chunks.append({
                            "content": page["content"].strip(),
                            "type": content_type,
                            "source": filename,
                            "page": page["page_num"]
                        })
                    else:
                        #print(f"Chunking page {page['page_num']} with content type: {content_type}")
                        chunks = self.text_splitter.split_text(page["content"])
                        #print(f"Split page into {len(chunks)} chunk(s)")
                        
                        for chunk in chunks:
                            all_chunks.append({
                                "content": chunk.strip(),
                                "type": content_type,
                                "source": filename,
                                "page": page["page_num"]
                            })
        
        return all_chunks

    # 4. Create vector store using OpenAI embeddings
    def create_vector_store(self, chunks: List[Dict[str, Any]]):
        """
        Takes in a list of text chunks and creates a Chroma vector store using 
        OpenAIEmbeddings. Stores the vector store as an instance variable.
        """
        #print("Creating vector store with OpenAI embeddings...")
        # Using the newer text-embedding-3-small model
        embedding_model = OpenAIEmbeddings(
            openai_api_key=self.openai_api_key,
            model="text-embedding-3-small"  # Using OpenAI's newer embedding model
        )
        
        # Create documents with detailed metadata
        docs = [
            Document(
                page_content=chunk["content"],
                metadata={
                    "type": chunk["type"],
                    "source": chunk["source"],
                    "page": chunk.get("page", 1)
                }
            )
            for chunk in chunks
        ]
        
        # Create vector store
        self.vectorstore = Chroma.from_documents(docs, embedding_model)
        #print(f"Vector store created successfully with {len(docs)} documents.")

    # 5. Create QA chain with DeepSeek LLM
    def create_qa_chain(self):
        """
        Creates the RetrievalQA chain using the Chroma vectorstore and the DeepSeek LLM.
        The chain is stored as an instance variable.
        """
        if not self.vectorstore:
            raise ValueError("Vector store not found. Please create it first by calling create_vector_store().")

        #print("Creating QA chain with DeepSeek LLM...")
        llm = ChatOpenAI(
            openai_api_key=self.deepseek_api_key,
            openai_api_base="https://api.deepseek.com/v1",
            model_name="deepseek-chat",
            temperature=0 
        )

        # Create QA chain with improved prompt
        self.qa_chain = RetrievalQA.from_chain_type(
            llm=llm,
            chain_type="stuff",
            retriever=self.vectorstore.as_retriever(
                search_type="similarity",
                search_kwargs={"k": 4}  # Increased from 2 to 4 for more context
            ),
            return_source_documents=True  # Include source documents in response
        )
        #print("QA Chain is ready.")

    # 6. Run a single query
    def run_query(self, query: str) -> Dict[str, Any]:
        """
        Runs the provided query through the RetrievalQA chain and returns the results.
        """
        if not self.qa_chain:
            raise ValueError("QA chain not initialized. Please call create_qa_chain() first.")

        print("Running query through QA chain...")
        result = self.qa_chain.invoke({"query": query})
        
        return result

    # 7. Test pipeline with predefined queries
    def test_pipeline(self, query: str):
        """
        Loads, chunks, and creates the vector store. Then runs the test query
        through the QA chain, printing out the results and their sources.
        """
        #print("Starting test pipeline...")
        chunks = self.load_and_chunk_pdfs()
        #print(f"Total chunks created: {len(chunks)}")

        self.create_vector_store(chunks)
        self.create_qa_chain()

        #print("=== RAG Pipeline Test ===")
        
        system_prompt = f"""
        You are a RAG agent that just retrieves the information that is asked for.
        """
        query = system_prompt + ": " + query
        
        #print(f"\nTest Query: {query}")
        result = self.run_query(query)
        answer = result["result"]
        source_docs = result["source_documents"]
        
        #print(f"Answer:\n{answer}\n")
        #print("Sources:")
        for j, doc in enumerate(source_docs, 1):
            source = doc.metadata.get("source", "Unknown")
            page = doc.metadata.get("page", "Unknown")
        #    print(f"{j}. {source} (Page {page})")
        
        return answer