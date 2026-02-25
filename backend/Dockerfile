FROM python:3.7

RUN apt-get -y update && apt-get install -y --no-install-recommends \
        default-jdk \
        && rm -rf /var/lib/apt/lists/*

RUN pip install virtualenv
ENV VIRTUAL_ENV=/venv
RUN virtualenv venv -p python3
ENV PATH="VIRTUAL_ENV/bin:$PATH"

COPY requirements.txt .
RUN pip install -r requirements.txt
#RUN pip install boto3
COPY . /opt/program
WORKDIR /opt/program

# Expose port 
EXPOSE 8200

# Run the application:
#CMD ["gunicorn","--workers=2", "--bind", "0.0.0.0:8200", "--log-level=debug", "wsgi:app"]
CMD ["python","src.py"]
