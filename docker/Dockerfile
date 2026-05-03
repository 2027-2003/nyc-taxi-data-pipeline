FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --timeout 300 --retries 5 -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/dashboard_app.py"] 
