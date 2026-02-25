from fastapi import APIRouter
from database import db

router = APIRouter()

@router.get("/users/summary")
async def get_users_summary():
    users_collection = db["users"]

    users = await users_collection.find().to_list(length=None)

    formatted_users = []

    for user in users:
        name = user.get("fullName")
        user_id = str(user.get("_id"))

        formatted_users.append({
            "displayName": name if name else user_id
        })

    return {
        "count": len(formatted_users),
        "users": formatted_users
    }