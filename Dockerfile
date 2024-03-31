FROM python:3.12.1

# Copy Files
WORKDIR /usr/src/app
COPY requirements.txt requirements.txt
COPY src src
COPY setup.py setup.py

# Install
RUN pip install .
RUN pip install -r requirements.txt

# Docker Run Command
EXPOSE 80
CMD [ "python", "-m", "flask", "--app", "mdm_python.backend_server.app:app", "run", "--host=0.0.0.0", "--port=80"]