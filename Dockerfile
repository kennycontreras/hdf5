FROM python:3

COPY ./docker-entrypoint.sh /
COPY ./hdf5.py /
COPY ./requirements.txt /

RUN pip3 install -r requirements.txt 
RUN chmod +x /docker-entrypoint.sh
RUN chmod +x /hdf5.py

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["python3", "/hdf5.py"] 
