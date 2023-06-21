FROM python:3.9-slim as compiler
ENV PYTHONUNBUFFERED 1

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY CustomControllerBasic/requirements.txt .
RUN pip install -r requirements.txt



FROM python:3.9-slim as build-image
COPY --from=compiler /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY edgerun .
COPY CustomControllerBasic .
RUN pip install -e faas
RUN pip install -e galileo-faas
RUN pip install -e faas-optimizations
CMD ["python", "-u", "main.py"]