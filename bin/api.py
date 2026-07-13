import uvicorn

from src.fastapi import create_app
from src.logging_config import configure_logging

configure_logging()
app = create_app()

if __name__ == "__main__":
    uvicorn.run("bin.api:app", host="0.0.0.0", port=8000, reload=True)
