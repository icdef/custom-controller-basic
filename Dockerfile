FROM python:3.9
COPY CustomControllerBasic ./
COPY edgerun ./edgerun/
RUN pip install -e edgerun/faas
RUN pip install -e edgerun/galileo-faas
RUN pip install -e edgerun/faas-optimizations
RUN pip install protobuf==3.20.*
RUN pip install edgerun-galileo-experiments==0.0.2.dev13
RUN pip install edgerun-galileo-experiments-extensions==0.0.1.dev13
CMD ["python", "-u", "./main.py"]