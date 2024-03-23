FROM python:3.11

WORKDIR /usr/src
COPY . .
RUN pip install -r ./requirements.txt
CMD ["python", "src/handler.py"]