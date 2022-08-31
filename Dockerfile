FROM python:3.9
WORKDIR /code

COPY ./requirements.txt /code/requirements.txt
RUN python -m pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY . /code/

WORKDIR /code/stack_api

EXPOSE 8000
ENV PORT 8000

CMD ["python","-m","uvicorn", "api_web:app", "--host", "0.0.0.0", "--port", "8000"]