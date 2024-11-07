FROM python:3.12
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip3 install -r requirements.txt
COPY . .
ENV PORT=8000
ENV INSTANCE_CONNECTION_NAME='cs-493-a3-440723:us-central1:a3-db'
ENV DB_NAME='a3' 
ENV DB_USER='a3-user-try'
ENV DB_PASS='0000'
ENV GOOGLE_APPLICATION_CREDENTIALS='./last-try.json'
EXPOSE ${PORT}
CMD [ "python", "main.py" ]
