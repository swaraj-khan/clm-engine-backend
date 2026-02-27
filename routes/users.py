from fastapi import APIRouter, Query, HTTPException
from database import db
from datetime import datetime, timedelta
from bson import ObjectId
import json

router = APIRouter()

def convert_objectid_to_str(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, dict):
        return {key: convert_objectid_to_str(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectid_to_str(item) for item in obj]
    else:
        return obj

VALID_STAGES = {
    "DEPLOYED",
    "OFFER_RECEIVED",
    "INTERVIEW_SCHEDULED",
    "ASSESSMENT_PENDING",
    "PROFILE_COMPLETE",
    "PROFILE_INCOMPLETE",
    "REGISTERED",
    "DORMANT",
    "DROPPED_OFF",
}


@router.get("/users")
async def get_users_by_stage(
    status: str = Query(...),
    limit: int = 50,
    skip: int = 0,
):
    status = status.upper()

    if status not in VALID_STAGES:
        raise HTTPException(status_code=400, detail="Invalid status")

    now = datetime.utcnow()
    thirty_days_ago = now - timedelta(days=30)
    one_eighty_days_ago = now - timedelta(days=180)

    pipeline = [
        {
            "$lookup": {
                "from": "appliedjobs",
                "localField": "_id",
                "foreignField": "userId",
                "as": "jobs",
            }
        },
        {
            "$addFields": {
                "educationSafe": {
                    "$ifNull": [
                        {
                            "$cond": [{"$isArray": "$education"}, "$education", None]
                        },
                        []
                    ]
                },
                "experienceSafe": {
                    "$ifNull": [
                        {
                            "$cond": [{"$isArray": "$experience"}, "$experience", None]
                        },
                        []
                    ]
                },
                "policiesSafe": {
                    "$ifNull": [
                        {
                            "$cond": [{"$isArray": "$acceptedPolicies"}, "$acceptedPolicies", None]
                        },
                        []
                    ]
                },
                "jobsSafe": {
                    "$ifNull": ["$jobs", []]
                },
            }
        },
        {
            "$addFields": {
                "hasTermsAccepted": {
                    "$gt": [
                        {
                            "$size": {
                                "$filter": {
                                    "input": "$policiesSafe",
                                    "as": "p",
                                    "cond": {
                                        "$eq": ["$$p.type", "TERMSANDCONDITIONS"]
                                    },
                                }
                            }
                        },
                        0,
                    ]
                },
                "isProfileComplete": {
                    "$and": [
                        {
                            "$gt": [
                                {
                                    "$size": {
                                        "$filter": {
                                            "input": "$policiesSafe",
                                            "as": "p",
                                            "cond": {
                                                "$eq": ["$$p.type", "TERMSANDCONDITIONS"]
                                            },
                                        }
                                    }
                                },
                                0,
                            ]
                        },
                        {"$gt": [{"$size": "$educationSafe"}, 0]},
                        {"$gt": [{"$size": "$experienceSafe"}, 0]},
                        {"$ne": [{"$ifNull": ["$fullName", ""]}, ""]},
                        {"$ne": ["$targetCountry", None]},
                        {"$ne": ["$targetJobRole", None]},
                    ]
                },
            }
        },
        {
            "$addFields": {
                "stage": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$lt": [{"$ifNull": ["$updatedAt", now]}, one_eighty_days_ago]
                                },
                                "then": "DROPPED_OFF",
                            },
                            {
                                "case": {
                                    "$lt": [{"$ifNull": ["$updatedAt", now]}, thirty_days_ago]
                                },
                                "then": "DORMANT",
                            },
                            {
                                "case": {
                                    "$in": ["OFFER_ACCEPTED", "$jobsSafe.applicationStatus"]
                                },
                                "then": "DEPLOYED",
                            },
                            {
                                "case": {
                                    "$in": ["OFFER_EXTENDED", "$jobsSafe.applicationStatus"]
                                },
                                "then": "OFFER_RECEIVED",
                            },
                            {
                                "case": {
                                    "$gt": [
                                        {
                                            "$size": {
                                                "$filter": {
                                                    "input": "$jobsSafe",
                                                    "as": "j",
                                                    "cond": {
                                                        "$in": [
                                                            "$$j.applicationStatus",
                                                            [
                                                                "INTERVIEW_SCHEDULED",
                                                                "INTERVIEW_COMPLETED",
                                                                "ADVANCED_TO_ROUND_2",
                                                                "ADVANCED_TO_ROUND_3",
                                                                "ADVANCED_TO_FINAL_ROUND",
                                                            ],
                                                        ]
                                                    },
                                                }
                                            }
                                        },
                                        0,
                                    ]
                                },
                                "then": "INTERVIEW_SCHEDULED",
                            },
                            {
                                "case": {
                                    "$gt": [
                                        {
                                            "$size": {
                                                "$filter": {
                                                    "input": "$jobsSafe",
                                                    "as": "j",
                                                    "cond": {
                                                        "$in": [
                                                            "$$j.applicationStatus",
                                                            ["UNDER_REVIEW", "SHORTLISTED"],
                                                        ]
                                                    },
                                                }
                                            }
                                        },
                                        0,
                                    ]
                                },
                                "then": "ASSESSMENT_PENDING",
                            },
                            {
                                "case": {"$eq": ["$isProfileComplete", True]},
                                "then": "PROFILE_COMPLETE",
                            },
                            {
                                "case": {"$eq": ["$hasTermsAccepted", True]},
                                "then": "PROFILE_INCOMPLETE",
                            },
                        ],
                        "default": "REGISTERED",
                    }
                }
            }
        },
        {"$match": {"stage": status}},
        {
            "$project": {
                "_id": {"$toString": "$_id"},
                "fullName": 1,
                "phoneNumber": 1,
                "userType": 1,
                "targetCountry": {
                    "name": "$targetCountry.name",
                    "id": {"$toString": "$targetCountry.id"},
                },
                "targetJobRole": {
                    "name": "$targetJobRole.name",
                    "id": {"$toString": "$targetJobRole.id"},
                },
                "isProfileCompleted": 1,
                "stage": 1,
                "createdAt": 1,
                "updatedAt": 1,
                "lastActivityAt": 1,
            }
        },
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "data": [{"$skip": skip}, {"$limit": limit}],
            }
        },
    ]

    result = await db.users.aggregate(pipeline).to_list(length=1)

    if not result:
        return {"status": status, "count": 0, "users": []}

    total = result[0]["metadata"][0]["total"] if result[0]["metadata"] else 0
    users = result[0]["data"]

    return {
        "status": status,
        "count": total,
        "users": users,
    }


@router.get("/users/applied-all")
async def get_all_users_who_applied(
    limit: int = 50,
    skip: int = 0,
):

    pipeline = [
        {
            "$group": {
                "_id": "$userId",
                "firstApplication": {"$first": "$$ROOT"}
            }
        },
        {
            "$replaceRoot": {
                "newRoot": "$firstApplication"
            }
        },
        {
            "$project": {
                "_id": 1,
                "userId": 1,
                "jobId": 1,
                "jobSnapshot": {
                    "location": "$jobSnapshot.location",
                    "salary": "$jobSnapshot.salary",
                    "company": "$jobSnapshot.company",
                    "contact": "$jobSnapshot.contact",
                    "jobRole": "$jobSnapshot.jobRole",
                    "jobCategory": "$jobSnapshot.jobCategory",
                    "_id": "$jobSnapshot._id",
                    "userId": "$jobSnapshot.userId",
                    "title": "$jobSnapshot.title",
                    "description": "$jobSnapshot.description",
                    "responsibilities": "$jobSnapshot.responsibilities",
                    "type": "$jobSnapshot.type",
                    "contractPeriod": "$jobSnapshot.contractPeriod",
                    "isActive": "$jobSnapshot.isActive",
                    "dateOfApplication": "$jobSnapshot.dateOfApplication",
                    "dateOfExpiration": "$jobSnapshot.dateOfExpiration",
                    "positions": "$jobSnapshot.positions",
                    "positionFilled": "$jobSnapshot.positionFilled",
                    "createdAt": "$jobSnapshot.createdAt",
                    "updatedAt": "$jobSnapshot.updatedAt",
                    "status": "$jobSnapshot.status",
                },
                "applicationStatus": 1,
                "resume": 1,
                "appliedAt": 1,
                "lastStatusUpdate": 1,
                "createdAt": 1,
                "updatedAt": 1,
            }
        },
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "data": [{"$skip": skip}, {"$limit": limit}],
            }
        },
    ]

    result = await db.appliedjobs.aggregate(pipeline).to_list(length=1)

    if not result:
        return {"count": 0, "applications": []}

    total = result[0]["metadata"][0]["total"] if result[0]["metadata"] else 0
    applications = result[0]["data"]

    applications = [convert_objectid_to_str(app) for app in applications]

    return {
        "count": total,
        "applications": applications,
    }