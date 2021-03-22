FROM python

RUN mkdir -p /usr/src/database

WORKDIR /usr/src/database

COPY . .

EXPOSE 5432

RUN pip3 install psycopg2

RUN python clean.py
# CMD ["python", "clean.py"]
RUN python create.py
# CMD ["python", "create.py"]