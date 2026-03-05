from fastapi import APIRouter, Query, HTTPException
from database import db
from datetime import datetime
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


@router.get("/ra/job-posted")
async def get_ra_job_posted(
    limit: int = 50,
    skip: int = 0,
):
    try:
        pipeline = [
            {"$match": {"userType": "employer"}},
            {
                "$lookup": {
                    "from": "jobs",
                    "localField": "_id",
                    "foreignField": "userId",
                    "as": "postedJobs",
                }
            },
            {
                "$addFields": {
                    "postedJobsCount": {"$size": {"$ifNull": ["$postedJobs", []]}},
                }
            },
            {
                "$match": {"postedJobsCount": {"$gt": 0}},
            },
            {
                "$project": {
                    "_id": {"$toString": "$_id"},
                    "fullName": 1,
                    "email": 1,
                    "phoneNumber": 1,
                    "company": 1,
                    "registrationNumber": 1,
                    "licenseNumber": 1,
                    "postedJobsCount": 1,
                    "createdAt": 1,
                }
            },
            {"$skip": skip},
            {"$limit": limit},
        ]

        ras = await db.employerusers.aggregate(pipeline).to_list(length=None)
        ras = [convert_objectid_to_str(r) for r in ras]

        if not ras:
            return {"count": 0, "ras": []}

        count_pipeline = [
            {"$match": {"userType": "employer"}},
            {
                "$lookup": {
                    "from": "jobs",
                    "localField": "_id",
                    "foreignField": "userId",
                    "as": "postedJobs",
                }
            },
            {
                "$addFields": {
                    "postedJobsCount": {"$size": {"$ifNull": ["$postedJobs", []]}},
                }
            },
            {"$match": {"postedJobsCount": {"$gt": 0}}},
            {"$count": "total"},
        ]
        count_result = await db.employerusers.aggregate(count_pipeline).to_list(length=1)
        total = count_result[0]["total"] if count_result else 0

        ra_ids = [ObjectId(r["_id"]) for r in ras]
        jobs_pipeline = [
            {"$match": {"userId": {"$in": ra_ids}}},
            {
                "$project": {
                    "_id": {"$toString": "$_id"},
                    "userId": {"$toString": "$userId"},
                    "title": 1,
                    "status": 1,
                    "country": 1,
                    "positions": 1,
                    "positionFilled": 1,
                    "createdAt": 1,
                }
            },
        ]

        jobs = await db.jobs.aggregate(jobs_pipeline).to_list(length=None)
        jobs = [convert_objectid_to_str(j) for j in jobs]

        jobs_by_ra = {}
        for job in jobs:
            uid = job.get("userId")
            if uid not in jobs_by_ra:
                jobs_by_ra[uid] = []
            jobs_by_ra[uid].append(job)

        for ra in ras:
            ra["jobs"] = jobs_by_ra.get(ra["_id"], [])

        return {
            "count": total,
            "ras": ras,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ra/candidate-submitted")
async def get_ra_candidate_submitted(
    limit: int = 50,
    skip: int = 0,
):
    try:

        pipeline = [
            {"$match": {"positionFilled": {"$gt": 0}}},
            {
                "$lookup": {
                    "from": "employerusers",
                    "localField": "userId",
                    "foreignField": "_id",
                    "as": "employer_info"
                }
            },
            {"$unwind": {"path": "$employer_info", "preserveNullAndEmptyArrays": True}},
            {
                "$project": {
                    "_id": {"$toString": "$_id"},
                    "userId": {"$toString": "$userId"},
                    "title": 1,
                    "status": 1,
                    "country": 1,
                    "positions": 1,
                    "positionFilled": 1,
                    "raName": "$employer_info.fullName",
                    "raCompany": "$employer_info.company.name",
                    "createdAt": 1
                }
            }
        ]
        
        jobs = await db.jobs.aggregate(pipeline).to_list(None)
        
        if not jobs:
            return {"count": 0, "submissions": []}
        
        jobs = [convert_objectid_to_str(j) for j in jobs]
        
        job_id_map = {}
        for job in jobs:
            job_id = job.get("_id")
            if job_id:
                job_id_map[job_id] = job

        if not job_id_map:
            return {"count": 0, "submissions": []}

        job_object_ids = [ObjectId(k) for k in job_id_map.keys()]
        applications = await db.appliedjobs.find({"jobId": {"$in": job_object_ids}}).to_list(None)
        applications = [convert_objectid_to_str(a) for a in applications]
        
        if not applications:
            return {"count": 0, "submissions": []}
        
        user_ids = [ObjectId(a.get("userId")) for a in applications if a.get("userId")]
        users = await db.users.find({"_id": {"$in": user_ids}}).to_list(None)
        users = [convert_objectid_to_str(u) for u in users]
        user_map = {u["_id"]: u for u in users}

        valid_applications = []
        for app in applications:
            app_job_id = str(app.get("jobId")) if app.get("jobId") else None
            job_info = job_id_map.get(app_job_id) if app_job_id else None
            
            if job_info:
                app["jobTitle"] = job_info.get("title") or "-"
                app["raName"] = job_info.get("raName") or "-"
                app["raCompany"] = job_info.get("raCompany") or "-"
                country_info = job_info.get("country")
                app["jobCountry"] = country_info.get("name") if country_info else "-"
                
                user_id = app.get("userId")
                if user_id and user_id in user_map:
                    user_info = user_map[user_id]
                    app["userName"] = user_info.get("fullName") or "-"
                    app["userPhone"] = user_info.get("phoneNumber") or "-"
                else:
                    app["userName"] = "-"
                    app["userPhone"] = "-"
                
                valid_applications.append(app)

        valid_applications.sort(key=lambda x: x.get("appliedAt", ""), reverse=True)
        
        total = len(valid_applications)
        paginated = valid_applications[skip:skip + limit]

        return {
            "count": total,
            "submissions": paginated,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ra/compliance-pending")
async def get_ra_compliance_pending(
    limit: int = 50,
    skip: int = 0,
):
    try:
        ras_pipeline = [
            {"$match": {"userType": "employer"}},
            {
                "$project": {
                    "_id": {"$toString": "$_id"},
                    "fullName": 1,
                    "email": 1,
                    "phoneNumber": 1,
                    "company": 1,
                    "registrationNumber": 1,
                    "licenseNumber": 1,
                    "acceptedPolicies": 1,
                    "createdAt": 1,
                }
            },
        ]

        ras = await db.employerusers.aggregate(ras_pipeline).to_list(length=None)
        ras = [convert_objectid_to_str(r) for r in ras]

        required_policies = ["PRIVACYPOLICY", "DISCLAIMER"]
        
        pending_compliance = []
        for ra in ras:
            accepted_types = [p.get("type") for p in ra.get("acceptedPolicies", [])]
            missing_policies = [p for p in required_policies if p not in accepted_types]
            
            if missing_policies:
                ra["missingPolicies"] = missing_policies
                pending_compliance.append(ra)

        total = len(pending_compliance)
        paginated = pending_compliance[skip:skip + limit]

        return {
            "count": total,
            "ras": paginated,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

