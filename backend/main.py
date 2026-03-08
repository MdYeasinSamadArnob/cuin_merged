# Vercel entry point — re-exports the FastAPI app from the api package
from api.main import app  # noqa: F401
