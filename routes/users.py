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

def rule_to_mongo(node):
    """
    Recursively convert rule/group tree into MongoDB filter.
    Applies smart normalization:
      - Same field eq under AND → converted to $in
    Works for scalar + array fields.
    """

    if not node or "type" not in node:
        return {}

    FIELD_MAP = {
        "userType": {"type": "scalar", "path": "userType"},
        "age": {"type": "scalar", "path": "age"},
        "targetCountry": {"type": "scalar", "path": "targetCountry.name"},
        "targetJobRole": {"type": "scalar", "path": "targetJobRole.name"},
        "daysInactive": {"type": "scalar", "path": "daysInactive"},
        "applicationStatus": {
            "type": "array",
            "arrayPath": "jobsSafe",
            "field": "applicationStatus",
        },
    }

    # --------------------------------------------------
    # RULE NODE
    # --------------------------------------------------
    if node["type"] == "rule":
        field = node.get("field")
        operator = node.get("operator")
        value = node.get("value")

        if value in (None, ""):
            return {}

        config = FIELD_MAP.get(field)
        if not config:
            return {}

        # numeric conversion
        if field in ("age", "daysInactive"):
            try:
                value = float(value)
            except Exception:
                return {}

        # ---------------- SCALAR ----------------
        if config["type"] == "scalar":
            path = config["path"]

            if operator == "eq":
                if isinstance(value, str):
                    return {path: {"$regex": f"^{value}$", "$options": "i"}}
                return {path: value}

            if operator == "neq":
                if isinstance(value, str):
                    return {path: {"$not": {"$regex": f"^{value}$", "$options": "i"}}}
                return {path: {"$ne": value}}

            if operator == "lt":
                return {path: {"$lt": value}}

            if operator == "lte":
                return {path: {"$lte": value}}

            if operator == "gt":
                return {path: {"$gt": value}}

            if operator == "gte":
                return {path: {"$gte": value}}

            if operator == "in":
                if isinstance(value, list):
                    items = value
                elif isinstance(value, str):
                    items = [v.strip() for v in value.split(",") if v.strip()]
                else:
                    items = [value]
                return {path: {"$in": items}}

            return {}

        # ---------------- ARRAY ----------------
        if config["type"] == "array":
            array_path = config["arrayPath"]
            sub_field = config["field"]

            if operator == "eq":
                return {
                    array_path: {
                        "$elemMatch": {sub_field: value}
                    }
                }

            if operator == "neq":
                return {
                    array_path: {
                        "$not": {
                            "$elemMatch": {sub_field: value}
                        }
                    }
                }

            if operator == "in":
                if isinstance(value, list):
                    items = value
                elif isinstance(value, str):
                    items = [v.strip() for v in value.split(",") if v.strip()]
                else:
                    items = [value]

                return {
                    array_path: {
                        "$elemMatch": {sub_field: {"$in": items}}
                    }
                }

            return {}

    # --------------------------------------------------
    # GROUP NODE
    # --------------------------------------------------
    if node["type"] == "group":
        operator = node.get("operator", "AND").upper()
        children = node.get("children", [])

        compiled_parts = []
        eq_merge_map = {}  # field -> list of values

        for child in children:
            if child.get("type") == "rule" and child.get("operator") == "eq" and operator == "AND":
                # collect for normalization
                field = child.get("field")
                value = child.get("value")
                eq_merge_map.setdefault(field, []).append(value)
            else:
                compiled = rule_to_mongo(child)
                if compiled:
                    compiled_parts.append(compiled)

        # 🔥 NORMALIZATION: same-field eq under AND → convert to IN
        if operator == "AND":
            for field, values in eq_merge_map.items():
                if len(values) == 1:
                    compiled = rule_to_mongo({
                        "type": "rule",
                        "field": field,
                        "operator": "eq",
                        "value": values[0],
                    })
                else:
                    compiled = rule_to_mongo({
                        "type": "rule",
                        "field": field,
                        "operator": "in",
                        "value": values,
                    })

                if compiled:
                    compiled_parts.append(compiled)

            if not compiled_parts:
                return {}

            if len(compiled_parts) == 1:
                return compiled_parts[0]

            return {"$and": compiled_parts}

        # OR case — no normalization
        else:
            parts = [rule_to_mongo(child) for child in children]
            parts = [p for p in parts if p]

            if not parts:
                return {}

            if len(parts) == 1:
                return parts[0]

            return {"$or": parts}

    return {}


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
        # lookup just the latest appliedAt timestamp via a pipeline
        {
            "$lookup": {
                "from": "appliedjobs",
                "let": {"userId": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$userId", "$$userId"]}}},
                    {"$group": {"_id": None, "latestAppliedAt": {"$max": "$appliedAt"}}}
                ],
                "as": "applicationMeta"
            }
        },
        {
            "$addFields": {
                "latestAppliedAt": {
                    "$ifNull": [
                        {"$arrayElemAt": ["$applicationMeta.latestAppliedAt", 0]},
                        None
                    ]
                },
                "educationSafe": {
                    "$ifNull": [
                        {"$cond": [{"$isArray": "$education"}, "$education", None]},
                        []
                    ]
                },
                "experienceSafe": {
                    "$ifNull": [
                        {"$cond": [{"$isArray": "$experience"}, "$experience", None]},
                        []
                    ]
                },
                "policiesSafe": {
                    "$ifNull": [
                        {"$cond": [{"$isArray": "$acceptedPolicies"}, "$acceptedPolicies", None]},
                        []
                    ]
                },
                "jobsSafe": {"$ifNull": ["$jobs", []]}
            }
        },
        {
            "$addFields": {
                # lastActivity is max of updatedAt and latestAppliedAt (ignoring
                # nulls). this is the reference point for daysInactive.
                "lastActivity": {
                    "$cond": [
                        {"$and": ["$latestAppliedAt", "$updatedAt"]},
                        {"$max": ["$updatedAt", "$latestAppliedAt"]},
                        {"$ifNull": ["$latestAppliedAt", "$updatedAt"]}
                    ]
                }
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
                                    "$lt": [{"$ifNull": ["$lastActivity", now]}, one_eighty_days_ago]
                                },
                                "then": "DROPPED_OFF",
                            },
                            {
                                "case": {
                                    "$lt": [{"$ifNull": ["$lastActivity", now]}, thirty_days_ago]
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


@router.get("/cohorts/options")
async def cohort_options():
    """Return dynamic values that populate rule builder dropdowns."""
    def uniquify(arr):
        seen = set()
        out = []
        for item in arr:
            if item is None:
                continue
            key = item.lower() if isinstance(item, str) else item
            if key not in seen:
                seen.add(key)
                out.append(item)
        return sorted(out, key=lambda x: x.lower() if isinstance(x, str) else x)

    user_types = uniquify(await db.users.distinct("userType"))
    countries = uniquify(await db.users.distinct("targetCountry.name"))
    job_roles = uniquify(await db.users.distinct("targetJobRole.name"))
    statuses = uniquify(await db.appliedjobs.distinct("applicationStatus"))
    return {
        "userTypes": user_types,
        "countries": countries,
        "jobRoles": job_roles,
        "applicationStatuses": statuses,
    }


@router.post("/cohorts/preview")
async def preview_cohort(rule: dict, limit: int = 100, skip: int = 0):
    try:
        match_expr = rule_to_mongo(rule)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

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
            "$lookup": {
                "from": "appliedjobs",
                "let": {"userId": "$_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$userId", "$$userId"]}}},
                    {"$group": {"_id": None, "latestAppliedAt": {"$max": "$appliedAt"}}}
                ],
                "as": "applicationMeta"
            }
        },
        {
            "$addFields": {
                "latestAppliedAt": {
                    "$ifNull": [
                        {"$arrayElemAt": ["$applicationMeta.latestAppliedAt", 0]},
                        None
                    ]
                },
                "educationSafe": {
                    "$ifNull": [
                        {"$cond": [{"$isArray": "$education"}, "$education", None]},
                        []
                    ]
                },
                "experienceSafe": {
                    "$ifNull": [
                        {"$cond": [{"$isArray": "$experience"}, "$experience", None]},
                        []
                    ]
                },
                "policiesSafe": {
                    "$ifNull": [
                        {"$cond": [{"$isArray": "$acceptedPolicies"}, "$acceptedPolicies", None]},
                        []
                    ]
                },
                "jobsSafe": {"$ifNull": ["$jobs", []]}
            }
        },
        {
            "$addFields": {
                "lastActivity": {
                    "$cond": [
                        {"$and": ["$latestAppliedAt", "$updatedAt"]},
                        {"$max": ["$updatedAt", "$latestAppliedAt"]},
                        {"$ifNull": ["$latestAppliedAt", "$updatedAt"]}
                    ]
                }
            }
        },
        {
            "$addFields": {
                "daysInactive": {
                    "$dateDiff": {
                        "startDate": "$lastActivity",
                        "endDate": "$$NOW",
                        "unit": "day"
                    }
                }
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
                                    "cond": {"$eq": ["$$p.type", "TERMSANDCONDITIONS"]},
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
                                            "cond": {"$eq": ["$$p.type", "TERMSANDCONDITIONS"]},
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
                                    "$lt": [{"$ifNull": ["$lastActivity", now]}, one_eighty_days_ago]
                                },
                                "then": "DROPPED_OFF",
                            },
                            {
                                "case": {
                                    "$lt": [{"$ifNull": ["$lastActivity", now]}, thirty_days_ago]
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
                                                        ],
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
    ]

    if match_expr:
        pipeline.append({"$match": match_expr})

    pipeline.append(
        {
            "$facet": {
                "metadata": [{"$count": "total"}],
                "data": [{"$skip": skip}, {"$limit": limit}],
            }
        }
    )

    result = await db.users.aggregate(pipeline).to_list(length=1)
    if not result:
        return {"count": 0, "sample": []}

    total = result[0]["metadata"][0]["total"] if result[0]["metadata"] else 0
    sample = result[0]["data"]
    sample = [convert_objectid_to_str(u) for u in sample]
    return {"count": total, "sample": sample}


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