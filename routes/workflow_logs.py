
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
from bson import ObjectId

router = APIRouter()

# MongoDB collection name
WORKFLOW_LOGS_COLLECTION = "workflow_logs"


def convert_objectid_to_str(obj):
    """Convert ObjectId to string for JSON serialization."""
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {key: convert_objectid_to_str(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid_to_str(item) for item in obj]
    else:
        return obj


# Log status enum
LOG_STATUS = {
    "TRIGGERED": "triggered",
    "EXECUTING": "executing",
    "COMPLETED": "completed",
    "FAILED": "failed",
    "SKIPPED": "skipped"
}


# Pydantic models
class WorkflowLogCreate(BaseModel):
    workflow_id: str
    workflow_name: str
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    event_type: str
    node_id: Optional[str] = None
    node_type: Optional[str] = None
    status: str  # triggered, executing, completed, failed, skipped
    message: str
    details: Optional[Dict[str, Any]] = None


class WorkflowLogResponse(BaseModel):
    id: str
    workflow_id: str
    workflow_name: str
    user_id: Optional[str] = None
    user_email: Optional[str] = None
    event_type: str
    node_id: Optional[str] = None
    node_type: Optional[str] = None
    status: str
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime


@router.post("/workflows/{workflow_id}/logs")
async def create_workflow_log(workflow_id: str, log_data: WorkflowLogCreate):
    """Create a new workflow log entry."""
    from database import db
    
    now = datetime.utcnow()
    
    log_entry = {
        "workflow_id": workflow_id,
        "workflow_name": log_data.workflow_name,
        "user_id": log_data.user_id,
        "user_email": log_data.user_email,
        "event_type": log_data.event_type,
        "node_id": log_data.node_id,
        "node_type": log_data.node_type,
        "status": log_data.status,
        "message": log_data.message,
        "details": log_data.details or {},
        "timestamp": now
    }
    
    result = await db[WORKFLOW_LOGS_COLLECTION].insert_one(log_entry)
    
    log_entry["id"] = str(result.inserted_id)
    
    return {"log": convert_objectid_to_str(log_entry), "message": "Log created successfully"}


@router.get("/workflows/{workflow_id}/logs")
async def get_workflow_logs(workflow_id: str, limit: int = 50):
    """Get logs for a specific workflow."""
    from database import db
    
    try:
        # Verify workflow exists
        from bson import ObjectId
        workflow = await db["workflows"].find_one({"_id": ObjectId(workflow_id)})
        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    
    logs = await db[WORKFLOW_LOGS_COLLECTION].find(
        {"workflow_id": workflow_id}
    ).sort("timestamp", -1).limit(limit).to_list(length=limit)
    
    # Convert ObjectId to string
    result = []
    for log in logs:
        log_data = convert_objectid_to_str(log)
        log_data["id"] = str(log["_id"])
        result.append(log_data)
    
    return {"logs": result}


@router.get("/workflows/logs/recent")
async def get_recent_logs(limit: int = 100):
    """Get recent logs across all active workflows."""
    from database import db
    
    logs = await db[WORKFLOW_LOGS_COLLECTION].find(
        {"status": {"$in": ["triggered", "executing"]}}
    ).sort("timestamp", -1).limit(limit).to_list(length=limit)
    
    # Convert ObjectId to string
    result = []
    for log in logs:
        log_data = convert_objectid_to_str(log)
        log_data["id"] = str(log["_id"])
        result.append(log_data)
    
    return {"logs": result}


@router.delete("/workflows/{workflow_id}/logs")
async def clear_workflow_logs(workflow_id: str):
    """Clear all logs for a specific workflow."""
    from database import db
    
    try:
        from bson import ObjectId
        workflow = await db["workflows"].find_one({"_id": ObjectId(workflow_id)})
        if not workflow:
            raise HTTPException(status_code=404, detail="Workflow not found")
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    
    result = await db[WORKFLOW_LOGS_COLLECTION].delete_many({"workflow_id": workflow_id})
    
    return {"message": f"Cleared {result.deleted_count} log entries"}


# Helper function to log workflow execution
async def log_workflow_execution(workflow_id: str, workflow_name: str, event_type: str, 
                                  status: str, message: str, user_id: str = None, 
                                  user_email: str = None, node_id: str = None, 
                                  node_type: str = None, details: dict = None):
    """Helper function to log workflow execution details."""
    from database import db
    
    now = datetime.utcnow()
    
    log_entry = {
        "workflow_id": workflow_id,
        "workflow_name": workflow_name,
        "user_id": user_id,
        "user_email": user_email,
        "event_type": event_type,
        "node_id": node_id,
        "node_type": node_type,
        "status": status,
        "message": message,
        "details": details or {},
        "timestamp": now
    }
    
    await db[WORKFLOW_LOGS_COLLECTION].insert_one(log_entry)

