FROM python:3.7.2
ENV PYTHONIOENCODING utf-8

COPY . /code/

RUN pip install flake8


WORKDIR /code/


CMD ["python", "-u", "/code/component.py"]
