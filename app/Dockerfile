FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi[standard]==0.115.* \
    uvicorn[standard]==0.34.* \
    asyncpg==0.30.* \
    pydantic==2.*

COPY . /app/app/

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
