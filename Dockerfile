FROM python:3.8
# Or any preferred Python version.
RUN pip install kubernetes python-dotenv
COPY . ./
CMD ["python", "-u", "./main.py"]
# Or enter the name of your unique directory and parameter set.