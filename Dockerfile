FROM --platform=linux/amd64 python:3.11
WORKDIR /code


ENV AM_I_IN_A_DOCKER_CONTAINER Yes

COPY ./requirements.txt /code/requirements.txt
RUN python3.11 -m pip install --no-cache-dir --upgrade -r /code/requirements.txt

RUN apt-get update -y
RUN apt install libgl1-mesa-glx -y
RUN apt-get install 'ffmpeg'\
    'libsm6'\
    'libxext6'  -y

COPY . /code/

WORKDIR /code/stack_api

EXPOSE 8000
ENV PORT 8000

CMD ["python3.11","-m","uvicorn", "api_web:app", "--host", "0.0.0.0", "--port", "8000"]