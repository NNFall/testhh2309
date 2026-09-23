FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --only-binary=:all: -r requirements.txt

COPY *.py ./
RUN test -f showcase.py

USER 10001:10001
EXPOSE 8000
CMD ["python", "showcase.py"]
