# this is an official Python runtime, used as the parent image
FROM python:3.9-alpine

RUN mkdir /app
COPY ./*.txt ./*.py ./*.sh ./*.onnx /app/

# execute everyone's favorite pip command, pip install -r
RUN cd /app && pip install --trusted-host pypi.python.org -r requirements.txt


# set the working directory in the container to /app
WORKDIR /app

# # add the current directory to the container as /app
# ADD . /app



# unblock port 80 for the Bottle app to run on
#EXPOSE 8000

# execute the Flask app
CMD ["python3", "oss_sync_server.py"]