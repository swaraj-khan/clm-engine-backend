from fastapi import FastAPI
from routes.users import router as users_router
from routes.employer import router as employer_router

app = FastAPI()
app.include_router(users_router)
app.include_router(employer_router)
