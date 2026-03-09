from fastapi import FastAPI
from routes.users import router as users_router
from routes.employer import router as employer_router
from routes.workflows import router as workflows_router

app = FastAPI()
app.include_router(users_router)
app.include_router(employer_router)
app.include_router(workflows_router)
