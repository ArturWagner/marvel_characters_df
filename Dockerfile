FROM python:3.8

COPY marvel_characters_df.py requirements.txt /opt/app/

WORKDIR /opt/app

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "marvel_characters_df.py"]