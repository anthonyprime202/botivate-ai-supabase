import os

from fastapi import FastAPI, BackgroundTasks, Request, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict
from langchain_core.messages import AIMessage, HumanMessage
from dotenv import load_dotenv
from contextlib import asynccontextmanager

from agent import agent
from script import sync_to_db

load_dotenv()

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")

class ChatRequest(BaseModel):
    question: str 
    chat_history: List[Dict[str, str]] # e.g., [{"type": "human", "content": "hi"}]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code here runs on startup
    print("🚀 Application startup: Running initial database sync...")
    sync_to_db()
    print("✅ Initial database sync complete. Application is ready.")
    
    yield
    
    # Code here runs on shutdown (optional)
    print("👋 Application shutdown.")

app = FastAPI(title="Botivate Rag Agent API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.get("/")
async def root():
    return {"status": "ok"}

@app.head("/")
async def status_check(response: Response):
    return Response(status_code=200)

@app.post("/chat")
async def chat_with_agent(request: ChatRequest):
    """
    The main endpoint to interact with the agent.
    It accepts a question and the conversation history.
    """

    history_messages = []

    for msg in request.chat_history:
        if msg.get("type") == "human":
            history_messages.append(HumanMessage(content=msg.get("content")))
        elif msg.get("type") == "ai":
            history_messages.append(AIMessage(content=msg.get("content")))
        
    initial_state = {
        "question": request.question,
        "chat_history": history_messages
    }

    final_state = agent.invoke(initial_state)

    return {"answer": final_state.get('answer', "Sorry, I encountered an error.")}


@app.post("/webhook/sync")
async def sync_db(request: Request, background_tasks: BackgroundTasks):
    """
    Webhook to receive update notifications from Google Apps Script.
    It runs the database sync process in the background.
    """
    # Security: Check for a secret token in the headers
    auth_token = request.headers.get('X-Webhook-Secret')
    if not WEBHOOK_SECRET or auth_token != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden: Invalid or missing token")

    print("✅ Webhook received. Starting DB sync in the background.")
    # Add the sync function as a background task
    background_tasks.add_task(sync_to_db)
    
    # Immediately return a response so Google Script isn't kept waiting
    return {"message": "Database update process started in the background."}


