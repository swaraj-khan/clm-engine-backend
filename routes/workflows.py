from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
from bson import ObjectId
import json

router = APIRouter()

# MongoDB collection name
WORKFLOWS_COLLECTION = "workflows"

# Workflow status enum
WORKFLOW_STATUS = {
    "DRAFT": "draft",
    "ACTIVE": "active",
    "PAUSED": "paused"
}

# Node types
NODE_TYPES = {
    "TRIGGER": "trigger",
    "DELAY": "delay",
    "CHANNEL": "channel",
    "CONDITION": "condition",
    "AB_SPLIT": "ab_split",
    "COHORT_MOVEMENT": "cohort_movement",
    "EXIT": "exit"
}

# Valid trigger events
TRIGGER_EVENTS = [
    "USER_REGISTERED",
    "PROFILE_INCOMPLETE",
    "PROFILE_COMPLETE",
    "APPLICATION_SUBMITTED",
    "INTERVIEW_SCHEDULED",
    "OFFER_RECEIVED",
    "OFFER_ACCEPTED",
    "OFFER_REJECTED",
    "DEPLOYED",
    "DORMANT",
    "DROPPED_OFF"
]

# Valid channels
COMMUNICATION_CHANNELS = [
    "WHATSAPP",
    "SMS",
    "EMAIL",
    "AI_VOICE_CALL",
    "PUSH_NOTIFICATION"
]


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


# Pydantic models
class TriggerNodeConfig(BaseModel):
    event: str
    cohort_id: Optional[str] = None


class DelayNodeConfig(BaseModel):
    duration: int  # in hours
    unit: str = "hours"  # hours, days


class ChannelNodeConfig(BaseModel):
    channel: str  # WHATSAPP, SMS, EMAIL, AI_VOICE_CALL, PUSH_NOTIFICATION
    template_id: Optional[str] = None
    message: Optional[str] = None


class ConditionNodeConfig(BaseModel):
    field: str
    operator: str  # eq, neq, contains, gt, lt, gte, lte
    value: Any


class ABSplitNodeConfig(BaseModel):
    percentage_a: int  # 0-100
    percentage_b: int = 50  # default 50-50 split


class CohortMovementNodeConfig(BaseModel):
    target_cohort: str  # Stage name like "PROFILE_COMPLETE", "DEPLOYED", etc.


class ExitNodeConfig(BaseModel):
    reason: Optional[str] = None


class NodeConfig(BaseModel):
    trigger: Optional[TriggerNodeConfig] = None
    delay: Optional[DelayNodeConfig] = None
    channel: Optional[ChannelNodeConfig] = None
    condition: Optional[ConditionNodeConfig] = None
    ab_split: Optional[ABSplitNodeConfig] = None
    cohort_movement: Optional[CohortMovementNodeConfig] = None
    exit: Optional[ExitNodeConfig] = None


class WorkflowNode(BaseModel):
    id: str
    type: str  # trigger, delay, channel, condition, ab_split, cohort_movement, exit
    position: Dict[str, float]  # x, y coordinates
    data: Optional[Dict[str, Any]] = None  # Node data including label, event, etc.
    config: Optional[NodeConfig] = None
    label: Optional[str] = None


class WorkflowEdge(BaseModel):
    id: str
    source: str  # source node id
    target: str  # target node id
    source_handle: Optional[str] = None  # for conditions: "true" or "false"
    target_handle: Optional[str] = None


class Workflow(BaseModel):
    name: str
    description: Optional[str] = None
    version: int = 1
    status: str = "draft"  # draft, active, paused
    assigned_cohort: Optional[str] = None
    nodes: List[WorkflowNode] = []
    edges: List[WorkflowEdge] = []
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class WorkflowUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    assigned_cohort: Optional[str] = None
    nodes: Optional[List[WorkflowNode]] = None
    edges: Optional[List[WorkflowEdge]] = None


@router.get("/workflows")
async def list_workflows():
    """List all workflows."""
    from database import db
    
    workflows = await db[WORKFLOWS_COLLECTION].find().sort("updated_at", -1).to_list(length=100)
    
    # Convert ObjectId to string - KEEP _id for frontend use
    result = []
    for wf in workflows:
        wf_data = convert_objectid_to_str(wf)
        # Keep _id field for edit/delete operations
        result.append(wf_data)
    
    return {"workflows": result}


@router.get("/workflows/{workflow_id}")
async def get_workflow(workflow_id: str):
    """Get a specific workflow by ID."""
    from database import db
    
    try:
        workflow = await db[WORKFLOWS_COLLECTION].find_one({"_id": ObjectId(workflow_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    wf_data = convert_objectid_to_str(workflow)
    # Keep _id for frontend use
    
    return wf_data


@router.post("/workflows")
async def create_workflow(workflow: Workflow):
    """Create a new workflow."""
    from database import db
    
    now = datetime.utcnow()
    
    # Convert nodes to dict format
    nodes_data = []
    for node in workflow.nodes:
        node_dict = {
            "id": node.id,
            "type": node.type,
            "position": node.position,
            "data": node.data or {"label": node.label} if node.label else {}
        }
        if node.label:
            node_dict["data"]["label"] = node.label
        nodes_data.append(node_dict)
    
    workflow_data = {
        "name": workflow.name,
        "description": workflow.description or "",
        "version": 1,
        "status": "draft",
        "assigned_cohort": workflow.assigned_cohort,
        "nodes": nodes_data,
        "edges": [edge.dict() for edge in workflow.edges],
        "created_at": now,
        "updated_at": now
    }
    
    result = await db[WORKFLOWS_COLLECTION].insert_one(workflow_data)
    
    workflow_data["_id"] = str(result.inserted_id)
    
    return {"workflow": workflow_data, "message": "Workflow created successfully"}


@router.put("/workflows/{workflow_id}")
async def update_workflow(workflow_id: str, workflow_update: WorkflowUpdate):
    """Update an existing workflow."""
    from database import db
    
    try:
        existing = await db[WORKFLOWS_COLLECTION].find_one({"_id": ObjectId(workflow_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    
    if not existing:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    # Build update dict
    update_data = {"updated_at": datetime.utcnow()}
    
    if workflow_update.name is not None:
        update_data["name"] = workflow_update.name
    if workflow_update.description is not None:
        update_data["description"] = workflow_update.description
    if workflow_update.status is not None:
        if workflow_update.status not in ["draft", "active", "paused"]:
            raise HTTPException(status_code=400, detail="Invalid status. Must be draft, active, or paused")
        update_data["status"] = workflow_update.status
    if workflow_update.assigned_cohort is not None:
        update_data["assigned_cohort"] = workflow_update.assigned_cohort
    if workflow_update.nodes is not None:
        # Convert nodes to dict format
        nodes_data = []
        for node in workflow_update.nodes:
            node_dict = {
                "id": node.id,
                "type": node.type,
                "position": node.position,
                "data": node.data or {"label": node.label} if node.label else {}
            }
            if node.label:
                node_dict["data"]["label"] = node.label
            nodes_data.append(node_dict)
        update_data["nodes"] = nodes_data
    if workflow_update.edges is not None:
        update_data["edges"] = [edge.dict() for edge in workflow_update.edges]
    
    await db[WORKFLOWS_COLLECTION].update_one(
        {"_id": ObjectId(workflow_id)},
        {"$set": update_data}
    )
    
    # Return updated workflow - KEEP _id for frontend
    updated = await db[WORKFLOWS_COLLECTION].find_one({"_id": ObjectId(workflow_id)})
    wf_data = convert_objectid_to_str(updated)
    
    return {"workflow": wf_data, "message": "Workflow updated successfully"}


@router.delete("/workflows/{workflow_id}")
async def delete_workflow(workflow_id: str):
    """Delete a workflow."""
    from database import db
    
    try:
        result = await db[WORKFLOWS_COLLECTION].delete_one({"_id": ObjectId(workflow_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    return {"message": "Workflow deleted successfully"}


@router.post("/workflows/{workflow_id}/duplicate")
async def duplicate_workflow(workflow_id: str):
    """Duplicate a workflow."""
    from database import db
    
    try:
        existing = await db[WORKFLOWS_COLLECTION].find_one({"_id": ObjectId(workflow_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    
    if not existing:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    now = datetime.utcnow()
    
    # Create duplicate with new name and reset version
    duplicate_data = {
        "name": f"{existing['name']} (Copy)",
        "description": existing.get("description", ""),
        "version": 1,
        "status": "draft",
        "assigned_cohort": existing.get("assigned_cohort"),
        "nodes": existing.get("nodes", []),
        "edges": existing.get("edges", []),
        "created_at": now,
        "updated_at": now
    }
    
    result = await db[WORKFLOWS_COLLECTION].insert_one(duplicate_data)
    
    duplicate_data["_id"] = str(result.inserted_id)
    # Keep _id for frontend
    
    return {"workflow": duplicate_data, "message": "Workflow duplicated successfully"}


@router.post("/workflows/{workflow_id}/activate")
async def activate_workflow(workflow_id: str):
    """Activate a workflow."""
    from database import db
    
    try:
        existing = await db[WORKFLOWS_COLLECTION].find_one({"_id": ObjectId(workflow_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    
    if not existing:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    # Check if workflow has at least a trigger and exit node
    node_types = [node.get("type") for node in existing.get("nodes", [])]
    if "trigger" not in node_types:
        raise HTTPException(status_code=400, detail="Workflow must have a trigger node to activate")
    if "exit" not in node_types:
        raise HTTPException(status_code=400, detail="Workflow must have an exit node to activate")
    
    # Deactivate other workflows in the same cohort (only one active per cohort)
    if existing.get("assigned_cohort"):
        await db[WORKFLOWS_COLLECTION].update_many(
            {"assigned_cohort": existing["assigned_cohort"], "status": "active"},
            {"$set": {"status": "paused", "updated_at": datetime.utcnow()}}
        )
    
    # Activate this workflow
    await db[WORKFLOWS_COLLECTION].update_one(
        {"_id": ObjectId(workflow_id)},
        {"$set": {"status": "active", "updated_at": datetime.utcnow()}}
    )
    
    updated = await db[WORKFLOWS_COLLECTION].find_one({"_id": ObjectId(workflow_id)})
    wf_data = convert_objectid_to_str(updated)
    # Keep _id for frontend
    
    return {"workflow": wf_data, "message": "Workflow activated successfully"}


@router.post("/workflows/{workflow_id}/deactivate")
async def deactivate_workflow(workflow_id: str):
    """Deactivate a workflow (set to paused)."""
    from database import db
    
    try:
        existing = await db[WORKFLOWS_COLLECTION].find_one({"_id": ObjectId(workflow_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    
    if not existing:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    await db[WORKFLOWS_COLLECTION].update_one(
        {"_id": ObjectId(workflow_id)},
        {"$set": {"status": "paused", "updated_at": datetime.utcnow()}}
    )
    
    updated = await db[WORKFLOWS_COLLECTION].find_one({"_id": ObjectId(workflow_id)})
    wf_data = convert_objectid_to_str(updated)
    # Keep _id for frontend
    
    return {"workflow": wf_data, "message": "Workflow deactivated successfully"}


@router.post("/workflows/{workflow_id}/version")
async def create_new_version(workflow_id: str):
    """Create a new version of a workflow."""
    from database import db
    
    try:
        existing = await db[WORKFLOWS_COLLECTION].find_one({"_id": ObjectId(workflow_id)})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid workflow ID format")
    
    if not existing:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    # If workflow is active, deactivate it before creating new version
    new_status = "draft" if existing.get("status") == "active" else existing.get("status", "draft")
    
    now = datetime.utcnow()
    
    # Create new version
    new_version_data = {
        "name": existing["name"],
        "description": existing.get("description", ""),
        "version": existing.get("version", 1) + 1,
        "status": new_status,
        "assigned_cohort": existing.get("assigned_cohort"),
        "nodes": existing.get("nodes", []),
        "edges": existing.get("edges", []),
        "created_at": now,
        "updated_at": now,
        "previous_version": workflow_id  # Reference to previous version
    }
    
    result = await db[WORKFLOWS_COLLECTION].insert_one(new_version_data)
    
    new_version_data["_id"] = str(result.inserted_id)
    # Keep _id for frontend
    
    return {"workflow": new_version_data, "message": "New version created successfully"}


@router.get("/workflows/options/trigger-events")
async def get_trigger_events():
    """Get available trigger events."""
    return {"trigger_events": TRIGGER_EVENTS}


@router.get("/workflows/options/channels")
async def get_channels():
    """Get available communication channels."""
    return {"channels": COMMUNICATION_CHANNELS}


@router.get("/workflows/options/stages")
async def get_cohort_stages():
    """Get available cohort stages for movement."""
    return {
        "stages": [
            "REGISTERED",
            "PROFILE_INCOMPLETE",
            "PROFILE_COMPLETE",
            "ASSESSMENT_PENDING",
            "INTERVIEW_SCHEDULED",
            "OFFER_RECEIVED",
            "DEPLOYED",
            "DORMANT",
            "DROPPED_OFF"
        ]
    }

